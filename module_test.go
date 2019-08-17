package bigarray

import (
	"sync"
	"testing"
)

func RunBigArrayBasicTests(t *testing.T, opts ...Option) {
	t.Helper()

	opts = append(opts,
		PageSize(32),
		NumValues(64))

	ba, err := New(opts...)
	if err != nil {
		t.Errorf("New: error: %v", err)
		return
	}
	defer ba.Close()

	if 64 != ba.Len() {
		t.Errorf("BigArray.Len: expected 64, got %d", ba.Len())
	}

	value, err := ba.ValueAt(42)
	if err != nil {
		t.Errorf("BigArray.ValueAt 42 [1/2]: error: %v", err)
	}
	if value != 0 {
		t.Error("42: expected false, got true")
	}

	err = ba.SetValueAt(42, 0xcc)
	if err != nil {
		t.Errorf("BigArray.SetValueAt 42: error: %v", err)
	}

	value, err = ba.ValueAt(42)
	if err != nil {
		t.Errorf("BigArray.ValueAt 42 [2/2]: error: %v", err)
	}
	if value != 0xcc {
		t.Error("42: expected true, got false")
	}

	value, err = ba.ValueAt(43)
	if err != nil {
		t.Errorf("BigArray.ValueAt 43: error: %v", err)
	}
	if value != 0 {
		t.Error("43: expected false, got true")
	}

	value, err = ba.ValueAt(41)
	if err != nil {
		t.Errorf("BigArray.ValueAt 41: error: %v", err)
	}
	if value != 0 {
		t.Error("41: expected false, got true")
	}

	_, err = ba.ValueAt(0)
	if err != nil {
		t.Errorf("BigArray.ValueAt 0: error: %v", err)
	}

	_, err = ba.ValueAt(63)
	if err != nil {
		t.Errorf("BigArray.ValueAt 63: error: %v", err)
	}

	iter := ba.Iterate(0, ba.Len())
	defer iter.Close()
	n := uint64(0)
	for iter.Next() {
		index := iter.Index()
		if n != index {
			t.Errorf("BigArray.Iterate out of order: expected [%d], got [%d]", n, index)
		}
		iter.SetValue(index)
		n++
	}
	if err := iter.Close(); err != nil {
		t.Errorf("BigArray.Iterate: error: %v", err)
	}
	if n != ba.Len() {
		t.Errorf("BigArray.Iterate only produced %d values", n)
	}

	iter = ba.ReverseIterate(0, ba.Len())
	n = 0
	for iter.Next() {
		n++
		expect := ba.Len() - n
		index := iter.Index()
		if expect != index {
			t.Errorf("BigArray.ReverseIterate out of order: expected [%d], got [%d]", expect, index)
		}
		if iter.Value() != index {
			t.Errorf("BigArray.ReverseIterate gave wrong value at [%d]: expected %d, got %d", expect, index, iter.Value())
		}
	}
	if err := iter.Close(); err != nil {
		t.Errorf("BigArray.ReverseIterate: error: %v", err)
	}
	if n != ba.Len() {
		t.Errorf("BigArray.ReverseIterate only produced %d values", n)
	}

	for i := uint64(0); i < ba.Len(); i++ {
		if err := ba.SetValueAt(i, 0); err != nil {
			t.Errorf("BigArray.SetValueAt %d: error: %v", i, err)
		}
	}

	for i := uint64(0); i < ba.Len(); i++ {
		value, err := ba.ValueAt(i)
		if err != nil {
			t.Errorf("BigArray.ValueAt %d: error: %v", i, err)
			continue
		}
		if value != 0 {
			t.Errorf("BigArray.ValueAt %d: wrong value: expected 0, got %d", i, value)
		}
	}
}

func TestBigArray_InMemory(t *testing.T) {
	for _, bpv := range []byte{1, 2, 4, 8} {
		t.Logf("running tests with bpv=%d", bpv)
		RunBigArrayBasicTests(t,
			BytesPerValue(bpv))
	}
}

func TestBigArray_OnDisk_NoPool(t *testing.T) {
	for _, bpv := range []byte{1, 2, 4, 8} {
		t.Logf("running tests with bpv=%d", bpv)
		RunBigArrayBasicTests(t,
			BytesPerValue(bpv),
			OnDiskThreshold(0))
	}
}

func TestBigArray_OnDisk_WithPool(t *testing.T) {
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, 64)
		},
	}
	for _, bpv := range []byte{1, 2, 4, 8} {
		t.Logf("running tests with bpv=%d", bpv)
		RunBigArrayBasicTests(t,
			BytesPerValue(bpv),
			OnDiskThreshold(0),
			WithPool(pool))
	}
}
