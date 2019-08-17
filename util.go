package bigarray

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

func calcMaxToBPV(max uint64) byte {
	if max <= uint64(^uint8(0)) {
		return 1
	}
	if max <= uint64(^uint16(0)) {
		return 2
	}
	if max <= uint64(^uint32(0)) {
		return 4
	}
	return 8
}

func calcBPVToMax(bpv byte) uint64 {
	switch bpv {
	case 1:
		return uint64(^uint8(0))
	case 2:
		return uint64(^uint16(0))
	case 4:
		return uint64(^uint32(0))
	case 8:
		return ^uint64(0)
	default:
		panic(errors.New("BUG"))
	}
}

func removeFile(file File) error {
	type namer interface{ Name() string }
	name := file.(namer).Name()
	if err := os.Remove(name); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func debugImpl(ba BigArray) string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	ForEach(ba, func(index uint64, value uint64) error {
		if index > 0 {
			buf.WriteByte(' ')
		}
		if value == ^uint64(0) {
			buf.WriteByte('.')
		} else {
			fmt.Fprintf(&buf, "%d", value)
		}
		return nil
	})
	buf.WriteByte(']')
	return buf.String()
}

func copyFromImpl(dst, src BigArray) error {
	srcIter := src.Iterate(0, src.Len())
	needCloseSrc := true
	defer func() {
		if needCloseSrc {
			srcIter.Close()
		}
	}()

	dstIter := dst.Iterate(0, dst.Len())
	needCloseDst := true
	defer func() {
		if needCloseDst {
			dstIter.Close()
		}
	}()

	for srcIter.Next() && dstIter.Next() {
		dstIter.SetValue(srcIter.Value())
	}

	needCloseDst = false
	err := dstIter.Close()
	if err != nil {
		return err
	}

	needCloseSrc = false
	return srcIter.Close()
}
