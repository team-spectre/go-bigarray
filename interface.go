package bigarray

import (
	"errors"
	"io/ioutil"
)

// ErrClosedIterator is returned when Iterator.Close() is called multiple times
var ErrClosedIterator = errors.New("iterator is already closed")

// BigArray provides an interface for dealing with very large arrays that don't
// necessarily fit in memory.
type BigArray interface {
	// Frozen returns true if this array is read-only.
	Frozen() bool

	// MaxValue returns the maximum value allowed for any element.
	MaxValue() uint64

	// Len returns the number of elements in this array.
	Len() uint64

	// ValueAt returns the value at the given index.
	//
	// On-disk arrays have very slow random access.  If your accesses are
	// sequential or roughly sequential, you should consider using an
	// Iterator.
	ValueAt(uint64) (uint64, error)

	// SetValueAt replaces the value at the given index.
	//
	// On-disk arrays have very slow random access.  If your accesses are
	// sequential or roughly sequential, you should consider using an
	// Iterator.
	SetValueAt(uint64, uint64) error

	// Iterate returns an Iterator that starts at index (i) and stops at
	// index (j-1).
	Iterate(i, j uint64) Iterator

	// ReverseIterate returns an Iterator that starts at index (j-1) and
	// stops at index (i).
	ReverseIterate(i, j uint64) Iterator

	// CopyFrom replaces this array's elements with the elements of the
	// provided array.  The arrays must have the same length, and no
	// element in the source array may have a value that exceeds this
	// array's MaxValue().
	CopyFrom(BigArray) error

	// Truncate trims the array to the given length.
	Truncate(uint64) error

	// Freeze makes the array read-only.
	Freeze() error

	// Flush ensures that all pending writes have reached the OS.
	Flush() error

	// Close flushes any writes and frees the resources used by the array.
	Close() error

	// Debug generates a human-friendly string representing the values in
	// the array.
	Debug() string
}

// Iterator provides an interface for fast sequential access to a BigArray.
//
// The basic usage pattern is:
//
//   iter := array.Iterate(i, j)
//   for iter.Next() {
//     ... // call Index(), Value(), and/or SetValue()
//   }
//   err := iter.Close()
//   if err != nil {
//     ... // handle error
//   }
//
// Iterators are created in an indeterminate state; the caller must invoke
// Next() to advance to the first index.
//
type Iterator interface {
	// Next advances the iterator to the next index and returns true, or
	// returns false if the end of the iteration has been reached or if an
	// error has occurred.
	Next() bool

	// Skip(n) is equivalent to calling Next() n times, but faster.
	Skip(uint64) bool

	// Index returns the index of the current element.
	Index() uint64

	// Value returns the value of the current element.
	Value() uint64

	// SetValue replaces the value of the current element.
	SetValue(uint64)

	// Err returns the error which caused Next() to return false.
	Err() error

	// Flush ensures that all pending writes have reached the OS.
	Flush() error

	// Close flushes writes and frees the resources used by the iterator.
	Close() error
}

// New constructs a BigArray instance.
func New(opts ...Option) (BigArray, error) {
	var o options
	o.apply(opts...)
	o.populate()

	numBytes := o.numValues * uint64(o.bytesPerValue)
	if o.backingFile == nil && numBytes < o.diskThreshold {
		var ba BigArray
		switch o.bytesPerValue {
		case 1:
			ba = &inMemoryArray8{
				data: make([]byte, o.numValues),
				max:  byte(o.maxValue),
				ro:   o.isReadOnly,
			}

		case 2:
			ba = &inMemoryArray16{
				data: make([]uint16, o.numValues),
				max:  uint16(o.maxValue),
				ro:   o.isReadOnly,
			}

		case 4:
			ba = &inMemoryArray32{
				data: make([]uint32, o.numValues),
				max:  uint32(o.maxValue),
				ro:   o.isReadOnly,
			}

		case 8:
			ba = &inMemoryArray64{
				data: make([]uint64, o.numValues),
				max:  o.maxValue,
				ro:   o.isReadOnly,
			}

		default:
			panic("BUG")
		}
		return ba, nil
	}

	doc := false
	if o.backingFile == nil {
		var err error
		o.backingFile, err = ioutil.TempFile("", "tmp")
		if err != nil {
			return nil, err
		}
		err = o.backingFile.Truncate(int64(numBytes))
		if err != nil {
			removeFile(o.backingFile)
			return nil, err
		}
		doc = true
	}

	ba := &onDiskArray{
		f:     o.backingFile,
		p:     o.bufferPool,
		cache: make(map[uint64]*cachePage),
		num:   o.numValues,
		max:   o.maxValue,
		psz:   o.pageSize,
		bpv:   o.bytesPerValue,
		ro:    o.isReadOnly,
		doc:   doc,
	}
	return ba, nil
}
