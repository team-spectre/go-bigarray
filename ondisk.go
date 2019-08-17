package bigarray

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type cachePage struct {
	buf    []byte
	data   []byte
	off    uint64
	refcnt uint32
	dirty  bool
}

func (page *cachePage) contains(i, j uint64) bool {
	p := page.off
	q := page.off + uint64(len(page.data))
	return (i >= p && j <= q)
}

type onDiskArray struct {
	f     File
	p     *sync.Pool
	cache map[uint64]*cachePage
	num   uint64
	max   uint64
	psz   uint
	bpv   byte
	ro    bool
	doc   bool
}

func (ba *onDiskArray) Frozen() bool {
	return ba.ro
}

func (ba *onDiskArray) MaxValue() uint64 {
	return ba.max
}

func (ba *onDiskArray) Len() uint64 {
	return ba.num
}

func (ba *onDiskArray) compute(index uint64) (uint64, uint64) {
	psz := uint64(ba.psz)
	bpv := uint64(ba.bpv)
	offset := index * bpv
	rounded := (offset / psz) * psz
	offset -= rounded
	return rounded, offset
}

func (ba *onDiskArray) ValueAt(index uint64) (uint64, error) {
	if index >= ba.Len() {
		return ^uint64(0), io.EOF
	}

	var data []byte
	pageStart, offsetInPage := ba.compute(index)
	page, found := ba.cache[pageStart]
	if found {
		data = page.data[offsetInPage : offsetInPage+uint64(ba.bpv)]
	} else {
		var tmp [8]byte
		data = tmp[0:ba.bpv]
		offset := index * uint64(ba.bpv)

		_, err := ba.f.ReadAt(data, int64(offset))
		if err != nil {
			return ^uint64(0), err
		}
	}
	return bpvDecode(ba.bpv, data), nil
}

func (ba *onDiskArray) SetValueAt(index uint64, value uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if value > ba.MaxValue() {
		panic(fmt.Sprintf("value out of range: value %d vs max %d", value, ba.MaxValue()))
	}
	if index >= ba.Len() {
		return io.EOF
	}

	var tmp [8]byte
	data := tmp[0:ba.bpv]
	offset := index * uint64(ba.bpv)
	bpvEncode(ba.bpv, data, value)

	pageStart, offsetInPage := ba.compute(index)
	page, found := ba.cache[pageStart]
	if found {
		copy(page.data[offsetInPage:offsetInPage+uint64(ba.bpv)], data)
	}

	_, err := ba.f.WriteAt(data, int64(offset))
	return err
}

func (ba *onDiskArray) Iterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("onDiskArray.Iterate: i > j: i=%d j=%d", i, j))
	}
	return &onDiskIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
	}
}

func (ba *onDiskArray) ReverseIterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("onDiskArray.ReverseIterate: i > j: i=%d j=%d", i, j))
	}
	return &onDiskIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
		down: true,
	}
}

func (ba *onDiskArray) CopyFrom(src BigArray) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if src.Len() != ba.Len() {
		panic("big arrays are not equal in size")
	}
	return copyFromImpl(ba, src)
}

func (ba *onDiskArray) Truncate(length uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if length > ba.Len() {
		panic("cannot grow a big array")
	}
	if len(ba.cache) != 0 {
		panic("Truncate() with live iterators is undefined behavior")
	}
	lengthBytes := length * uint64(ba.bpv)
	ba.num = length
	return ba.f.Truncate(int64(lengthBytes))
}

func (ba *onDiskArray) Freeze() error {
	ba.ro = true
	return ba.Flush()
}

func (ba *onDiskArray) Flush() error {
	type flusher interface{ Flush() error }

	var finalError error
	for _, page := range ba.cache {
		if err := flushPage(ba, page); err != nil && finalError == nil {
			finalError = err
		}
	}
	if f, ok := ba.f.(flusher); ok {
		if err := f.Flush(); finalError == nil {
			finalError = err
		}
	}
	return finalError
}

func (ba *onDiskArray) Sync() error {
	type syncer interface{ Sync() error }

	if err := ba.Flush(); err != nil {
		return err
	}
	if f, ok := ba.f.(syncer); ok {
		return f.Sync()
	}
	return &NotImplementedError{Op: "Sync"}
}

func (ba *onDiskArray) Close() error {
	needClose := true
	defer func() {
		if needClose && ba.doc {
			removeFile(ba.f)
		} else if needClose {
			ba.f.Close()
		}
	}()

	if len(ba.cache) != 0 {
		panic("BigArray.Close called with outstanding iterators")
	}

	if ba.doc {
		needClose = false
		return removeFile(ba.f)
	}

	needClose = false
	return ba.f.Close()
}

func (ba *onDiskArray) Debug() string {
	return debugImpl(ba)
}

func (ba *onDiskArray) acquirePage(off uint64) (*cachePage, error) {
	page, found := ba.cache[off]
	if found {
		page.refcnt++
		return page, nil
	}

	var bb []byte
	if ba.p != nil {
		bb = ba.p.Get().([]byte)
	}

	var b []byte
	if uint(cap(bb)) >= ba.psz {
		b = bb[0:ba.psz]
	} else {
		b = make([]byte, ba.psz)
	}

	n, err := ba.f.ReadAt(b, int64(off))
	if err != nil && err != io.EOF {
		return nil, err
	}
	b = b[0:n]

	page = &cachePage{
		buf:    bb,
		data:   b,
		off:    off,
		refcnt: 1,
		dirty:  false,
	}
	ba.cache[off] = page
	return page, nil
}

func (ba *onDiskArray) disposePage(page *cachePage) {
	if page == nil {
		return
	}
	if page.dirty {
		panic("cannot dispose of a dirty page")
	}
	page.refcnt--
	if page.refcnt > 0 {
		return
	}
	delete(ba.cache, page.off)
	if ba.p != nil && page.buf != nil {
		ba.p.Put(page.buf)
	}
	*page = cachePage{}
}

var _ BigArray = (*onDiskArray)(nil)

type onDiskIterator struct {
	ba     *onDiskArray
	page   *cachePage
	err    error
	base   uint64
	pos    uint64
	num    uint64
	val    uint64
	primed bool
	down   bool
}

func (iter *onDiskIterator) Err() error { return iter.err }
func (iter *onDiskIterator) Next() bool { return iter.Skip(1) }

func (iter *onDiskIterator) Index() uint64 {
	if !iter.primed {
		panic("must call Next() before Index()")
	}
	if iter.pos >= iter.num {
		panic("must not call Index() after Next() returns false")
	}
	if iter.down {
		return iter.base + (iter.num - iter.pos - 1)
	}
	return iter.base + iter.pos
}

func (iter *onDiskIterator) Value() uint64 {
	if !iter.primed {
		panic("must call Next() before Value()")
	}
	if iter.pos >= iter.num {
		panic("must not call Value() after Next() returns false")
	}
	return iter.val
}

func (iter *onDiskIterator) SetValue(value uint64) {
	if !iter.primed {
		panic("must call Next() before SetValue()")
	}
	if iter.pos >= iter.num {
		panic("must not call SetValue() after Next() returns false")
	}
	if iter.ba.ro {
		panic("BigArray is read-only")
	}
	if value > iter.ba.MaxValue() {
		panic(fmt.Sprintf("value out of range: value %d vs max %d", value, iter.ba.MaxValue()))
	}
	if iter.err != nil {
		return
	}

	bpv := uint64(iter.ba.bpv)
	index := iter.Index()
	offset := index * bpv
	offset -= iter.page.off
	data := iter.page.data[offset : offset+bpv]
	bpvEncode(iter.ba.bpv, data, value)
	iter.page.dirty = true
}

func (iter *onDiskIterator) Skip(n uint64) bool {
	if iter.pos > iter.num {
		panic(fmt.Sprintf("iter.pos=%d iter.num=%d", iter.pos, iter.num))
	}
	if n == 0 && !iter.primed {
		panic("must call Next() before Step(0)")
	}
	if iter.err != nil {
		return false
	}
	if !iter.primed {
		n--
		iter.primed = true
	}
	if n >= (iter.num - iter.pos) {
		iter.pos = iter.num
		iter.val = ^uint64(0)
		return false
	}

	iter.pos += n

	bpv := uint64(iter.ba.bpv)
	psz := uint64(iter.ba.psz)
	offset := iter.Index() * bpv
	pageOffset := (offset / psz) * psz

	page := iter.page
	if page != nil && page.off != pageOffset {
		err := flushPage(iter.ba, page)
		if err != nil {
			iter.err = err
			iter.val = ^uint64(0)
			return false
		}
		iter.ba.disposePage(page)
		iter.page = nil
		page = nil
	}
	if page == nil {
		var err error
		page, err = iter.ba.acquirePage(pageOffset)
		if err != nil {
			iter.err = err
			iter.val = ^uint64(0)
			return false
		}
		iter.page = page
	}

	offset -= page.off
	data := page.data[offset : offset+bpv]
	iter.val = bpvDecode(iter.ba.bpv, data)
	return true
}

func (iter *onDiskIterator) Flush() error {
	return flushPage(iter.ba, iter.page)
}

func (iter *onDiskIterator) Close() error {
	err := iter.Flush()
	if iter.err != nil {
		err = iter.err
	}
	iter.ba.disposePage(iter.page)
	*iter = onDiskIterator{err: ErrClosedIterator}
	return err
}

var _ Iterator = (*onDiskIterator)(nil)

func bpvDecode(bpv byte, data []byte) uint64 {
	switch bpv {
	case 1:
		return uint64(data[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(data))
	case 4:
		return uint64(binary.LittleEndian.Uint32(data))
	case 8:
		return binary.LittleEndian.Uint64(data)
	default:
		panic("BUG")
	}
}

func bpvEncode(bpv byte, data []byte, value uint64) {
	switch bpv {
	case 1:
		data[0] = byte(value)
	case 2:
		binary.LittleEndian.PutUint16(data, uint16(value))
	case 4:
		binary.LittleEndian.PutUint32(data, uint32(value))
	case 8:
		binary.LittleEndian.PutUint64(data, value)
	default:
		panic("BUG")
	}
}

func flushPage(ba *onDiskArray, page *cachePage) error {
	if page != nil && page.dirty {
		_, err := ba.f.WriteAt(page.data, int64(page.off))
		if err != nil {
			return err
		}
		page.dirty = false
	}
	return nil
}
