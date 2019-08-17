package bigarray

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	defaultPageSize        = 16384     // 16 KiB
	defaultOnDiskThreshold = 268435456 // 256 MiB
)

type options struct {
	numValues          uint64
	maxValue           uint64
	diskThreshold      uint64
	backingFile        File
	bufferPool         *sync.Pool
	pageSize           uint
	bytesPerValue      byte
	diskThresholdIsSet bool
	isReadOnly         bool
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func (o *options) populate() {
	switch {
	case o.maxValue == 0 && o.bytesPerValue == 0:
		panic(errors.New("must specify at least one of MaxValue or BytesPerValue"))
	case o.maxValue == 0 && o.bytesPerValue != 0:
		o.maxValue = calcBPVToMax(o.bytesPerValue)
	case o.bytesPerValue == 0:
		o.bytesPerValue = calcMaxToBPV(o.maxValue)
	default:
		maxmax := calcBPVToMax(o.bytesPerValue)
		if o.maxValue > maxmax {
			panic(fmt.Errorf("MaxValue %d is greater than %d, which is the upper limit for BytesPerValue %d", o.maxValue, maxmax, o.bytesPerValue))
		}
	}

	if !o.diskThresholdIsSet {
		o.diskThreshold = defaultOnDiskThreshold
	}

	if o.pageSize == 0 {
		o.pageSize = defaultPageSize
	}
	if o.pageSize < uint(o.bytesPerValue) {
		panic(errors.New("PageSize must be at least as large as a single value"))
	}
	o.pageSize = (o.pageSize / uint(o.bytesPerValue)) * uint(o.bytesPerValue)
}

func (o options) debugString() string {
	hasFile := (o.backingFile != nil)
	hasPool := (o.bufferPool != nil)
	return fmt.Sprintf(
		"{num:%d max:%d bpv:%d odt:%d odtset:%v psz:%d file:%v pool:%v ro:%v}",
		o.numValues,
		o.maxValue,
		o.bytesPerValue,
		o.diskThreshold,
		o.diskThresholdIsSet,
		o.pageSize,
		hasFile,
		hasPool,
		o.isReadOnly)
}

// Option is a behavior customization for New.
type Option func(*options)

// NumValues specifies the length of the array to create.
//
// NumValues must be specified for all arrays.
//
func NumValues(size uint64) Option {
	return func(o *options) { o.numValues = size }
}

// MaxValue specifies the maximum value that any element will be able to have.
//
// At least one of MaxValue or BytesPerValue must be specified for all arrays.
//
func MaxValue(max uint64) Option {
	return func(o *options) { o.maxValue = max }
}

// BytesPerValue returns the number of bytes used to store each element's value.
//
// Must be one of {1, 2, 4, 8}, or 0 to use the default (computed from MaxValue).
//
// At least one of MaxValue or BytesPerValue must be specified for all arrays.
//
func BytesPerValue(bpv uint8) Option {
	if bpv != 0 && bpv != 1 && bpv != 2 && bpv != 4 && bpv != 8 {
		panic(errors.New("must specify 1, 2, 4, or 8 for BytesPerValue"))
	}
	return func(o *options) { o.bytesPerValue = bpv }
}

// OnDiskThreshold specifies the maximum memory usage (bytes) for an in-memory
// BigArray.  Arrays larger than this will be backed automatically by a
// temporary file.  The default is 256 MiB.
//
func OnDiskThreshold(size uint64) Option {
	return func(o *options) {
		o.diskThreshold = size
		o.diskThresholdIsSet = true
	}
}

// PageSize specifies the page size for disk I/O.  The created array's
// Iterators will load data from disk in blocks of this size.
//
// Must be divisible by 8 and should be at least 4096, or 0 to use the default
// (which is 16 KiB).
//
func PageSize(size uint) Option {
	return func(o *options) { o.pageSize = size }
}

// WithPool specifies a buffer pool to use for disk I/O.  The pool must contain
// []byte slices with a capacity at least as large as the value for PageSize.
//
func WithPool(pool *sync.Pool) Option {
	return func(o *options) { o.bufferPool = pool }
}

// WithFile specifies the read-write file handle which will back the array.
func WithFile(file File) Option {
	return func(p *options) { p.backingFile = file }
}

// WithReadOnlyFile specifies the read-only file handle which will back the array.
func WithReadOnlyFile(file io.ReaderAt) Option {
	return func(p *options) {
		p.backingFile = wrappedReaderAt{file}
		p.isReadOnly = true
	}
}
