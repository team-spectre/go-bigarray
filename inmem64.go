package bigarray

import (
	"fmt"
	"io"
)

type inMemoryArray64 struct {
	data []uint64
	max  uint64
	ro   bool
}

func (ba *inMemoryArray64) Frozen() bool {
	return ba.ro
}

func (ba *inMemoryArray64) MaxValue() uint64 {
	return ba.max
}

func (ba *inMemoryArray64) Len() uint64 {
	return uint64(len(ba.data))
}

func (ba *inMemoryArray64) ValueAt(index uint64) (uint64, error) {
	if index >= ba.Len() {
		return ^uint64(0), io.EOF
	}
	return ba.data[index], nil
}

func (ba *inMemoryArray64) SetValueAt(index uint64, value uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if value > ba.MaxValue() {
		panic(fmt.Sprintf("value out of range: value %d vs max %d", value, ba.MaxValue()))
	}
	if index >= ba.Len() {
		return io.EOF
	}
	ba.data[index] = value
	return nil
}

func (ba *inMemoryArray64) Iterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray64.Iterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
	}
}

func (ba *inMemoryArray64) ReverseIterate(i, j uint64) Iterator {
	if i > j {
		panic(fmt.Errorf("inMemoryArray64.ReverseIterate: i > j: i=%d j=%d", i, j))
	}
	return &inMemoryIterator{
		ba:   ba,
		base: i,
		num:  (j - i),
		val:  ^uint64(0),
		down: true,
	}
}

func (ba *inMemoryArray64) CopyFrom(src BigArray) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if src.Len() != ba.Len() {
		panic("big arrays are not equal in size")
	}
	if x, ok := src.(*inMemoryArray64); ok {
		copy(ba.data, x.data)
		return nil
	}
	return copyFromImpl(ba, src)
}

func (ba *inMemoryArray64) Truncate(n uint64) error {
	if ba.ro {
		panic("BigArray is read-only")
	}
	if n > ba.Len() {
		panic("cannot grow a big array")
	}
	ba.data = ba.data[0:n]
	return nil
}

func (ba *inMemoryArray64) Freeze() error {
	ba.ro = true
	return nil
}

func (ba *inMemoryArray64) Flush() error {
	return nil
}

func (ba *inMemoryArray64) Close() error {
	return nil
}

func (ba *inMemoryArray64) Debug() string {
	return debugImpl(ba)
}

var _ BigArray = (*inMemoryArray64)(nil)
