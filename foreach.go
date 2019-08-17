package bigarray

// ForEach is a convenience function that iterates over the entire array in the
// forward direction.
func ForEach(ba BigArray, fn func(uint64, uint64) error) error {
	iter := ba.Iterate(0, ba.Len())
	for iter.Next() {
		err := fn(iter.Index(), iter.Value())
		if err != nil {
			iter.Close()
			return err
		}
	}
	return iter.Close()
}

// ReverseForEach is a convenience function that iterates over the entire array
// in the reverse direction.
func ReverseForEach(ba BigArray, fn func(uint64, uint64) error) error {
	iter := ba.ReverseIterate(0, ba.Len())
	for iter.Next() {
		err := fn(iter.Index(), iter.Value())
		if err != nil {
			iter.Close()
			return err
		}
	}
	return iter.Close()
}
