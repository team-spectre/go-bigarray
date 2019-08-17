package bigarray

import (
	"fmt"
	"io"
)

type inMemoryArray32 struct {
	data []uint32
	max  uint32
	ro   bool
}

func (ba *inMemoryArray32) Frozen() bool {
	return ba.ro
}

func (ba *inMemoryArray32) MaxValue() uint64 {
	return uint64(ba.max)
}

func (ba *inMemoryArray32) Len() uint64 {
	return uint64(len(ba.data))
}

func (ba *inMemoryArray32) ValueAt(index uint64) (uint64, error) {
	if index >= ba.Len() {
		return ^uint64(0), io.EOF
	}
	return uint64(ba.data[index]), nil
}

func (ba *inMemoryArray32) SetValueAt(index uint64, value uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if value > ba.MaxValue() {
		panic(fmt.Sprintf("value out of range: value %d vs max %d", value, ba.MaxValue()))
	}
	if index >= ba.Len() {
		return io.EOF
	}
	ba.data[index] = uint32(value)
	return nil
}

func (ba *inMemoryArray32) Iterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray32.Iterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
	}
}

func (ba *inMemoryArray32) ReverseIterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray32.ReverseIterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
		down: true,
	}
}

func (ba *inMemoryArray32) CopyFrom(src BigArray) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if src.Len() != ba.Len() {
		panic("big arrays are not equal in size")
	}
	if x, ok := src.(*inMemoryArray32); ok {
		copy(ba.data, x.data)
		return nil
	}
	return copyFromImpl(ba, src)
}

func (ba *inMemoryArray32) Truncate(n uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if n > ba.Len() {
		panic("cannot grow a big array")
	}
	ba.data = ba.data[0:n]
	return nil
}

func (ba *inMemoryArray32) Freeze() error {
	ba.ro = true
	return nil
}

func (ba *inMemoryArray32) Flush() error {
	return nil
}

func (ba *inMemoryArray32) Close() error {
	return nil
}

func (ba *inMemoryArray32) Debug() string {
	return debugImpl(ba)
}

var _ BigArray = (*inMemoryArray32)(nil)
