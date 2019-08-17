package bigarray

import (
	"fmt"
	"io"
)

type inMemoryArray16 struct {
	data []uint16
	max  uint16
	ro   bool
}

func (ba *inMemoryArray16) Frozen() bool {
	return ba.ro
}

func (ba *inMemoryArray16) MaxValue() uint64 {
	return uint64(ba.max)
}

func (ba *inMemoryArray16) Len() uint64 {
	return uint64(len(ba.data))
}

func (ba *inMemoryArray16) ValueAt(index uint64) (uint64, error) {
	if index >= ba.Len() {
		return ^uint64(0), io.EOF
	}
	return uint64(ba.data[index]), nil
}

func (ba *inMemoryArray16) SetValueAt(index uint64, value uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if value > ba.MaxValue() {
		panic(fmt.Sprintf("value out of range: value %d vs max %d", value, ba.MaxValue()))
	}
	if index >= ba.Len() {
		return io.EOF
	}
	ba.data[index] = uint16(value)
	return nil
}

func (ba *inMemoryArray16) Iterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray16.Iterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
	}
}

func (ba *inMemoryArray16) ReverseIterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray16.ReverseIterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
		down: true,
	}
}

func (ba *inMemoryArray16) CopyFrom(src BigArray) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if src.Len() != ba.Len() {
		panic("big arrays are not equal in size")
	}
	if x, ok := src.(*inMemoryArray16); ok {
		copy(ba.data, x.data)
		return nil
	}
	return copyFromImpl(ba, src)
}

func (ba *inMemoryArray16) Truncate(n uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if n > ba.Len() {
		panic("cannot grow a big array")
	}
	ba.data = ba.data[0:n]
	return nil
}

func (ba *inMemoryArray16) Freeze() error {
	ba.ro = true
	return nil
}

func (ba *inMemoryArray16) Flush() error {
	return nil
}

func (ba *inMemoryArray16) Close() error {
	return nil
}

func (ba *inMemoryArray16) Debug() string {
	return debugImpl(ba)
}

var _ BigArray = (*inMemoryArray16)(nil)
