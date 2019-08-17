package bigarray

import (
	"fmt"
	"io"
)

type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Truncate(int64) error
}

// NotImplementedError is returned by method calls that aren't implemented for
// this file type.
type NotImplementedError struct {
	Op string
}

func (err *NotImplementedError) Error() string {
	return fmt.Sprintf("%s: not implemented", err.Op)
}

type wrappedReaderAt struct {
	r io.ReaderAt
}

func (wrat wrappedReaderAt) Wrapped() interface{} {
	return wrat.r
}

func (wrat wrappedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return wrat.r.ReadAt(p, off)
}

func (wrat wrappedReaderAt) WriteAt(p []byte, off int64) (int, error) {
	return 0, &NotImplementedError{Op: "WriteAt"}
}

func (wrat wrappedReaderAt) Truncate(n int64) error {
	return &NotImplementedError{Op: "Truncate"}
}

func (wrat wrappedReaderAt) Flush() error {
	return nil
}

func (wrat wrappedReaderAt) Close() error {
	return nil
}
