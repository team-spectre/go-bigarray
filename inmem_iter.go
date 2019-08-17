package bigarray

import (
	"fmt"
)

type inMemoryIterator struct {
	ba     BigArray
	err    error
	base   uint64
	pos    uint64
	num    uint64
	val    uint64
	primed bool
	down   bool
}

func (iter *inMemoryIterator) Err() error { return iter.err }
func (iter *inMemoryIterator) Next() bool { return iter.Skip(1) }

func (iter *inMemoryIterator) Index() uint64 {
	if !iter.primed {
		panic(fmt.Errorf("must call Next() before Index()"))
	}
	if iter.pos >= iter.num {
		panic(fmt.Errorf("must not call Index() after Next() returns false"))
	}
	if iter.down {
		return iter.base + (iter.num - iter.pos - 1)
	}
	return iter.base + iter.pos
}

func (iter *inMemoryIterator) Value() uint64 {
	if !iter.primed {
		panic(fmt.Errorf("must call Next() before Value()"))
	}
	if iter.pos >= iter.num {
		panic(fmt.Errorf("must not call Value() after Next() returns false"))
	}
	return iter.val
}

func (iter *inMemoryIterator) SetValue(value uint64) {
	if !iter.primed {
		panic(fmt.Errorf("must call Next() before SetValue()"))
	}
	if iter.pos >= iter.num {
		panic(fmt.Errorf("must not call SetValue() after Next() returns false"))
	}
	if iter.err != nil {
		return
	}
	iter.val = value
	iter.err = iter.ba.SetValueAt(iter.Index(), value)
}

func (iter *inMemoryIterator) Skip(n uint64) bool {
	if iter.pos > iter.num {
		panic(fmt.Sprintf("iter.pos=%d iter.num=%d", iter.pos, iter.num))
	}
	if n == 0 && !iter.primed {
		panic(fmt.Errorf("must call Next() before Skip(0)"))
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
	iter.val, iter.err = iter.ba.ValueAt(iter.Index())
	if iter.err != nil {
		iter.val = ^uint64(0)
		return false
	}
	return true
}

func (iter *inMemoryIterator) Flush() error {
	return nil
}

func (iter *inMemoryIterator) Close() error {
	err := iter.err
	*iter = inMemoryIterator{err: ErrClosedIterator}
	return err
}

var _ Iterator = (*inMemoryIterator)(nil)
