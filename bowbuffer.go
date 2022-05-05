package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v8/arrow/bitutil"
)

// Buffer is a mutable data structure with the purpose of easily building data Series with:
// - Data: slice of data.
// - nullBitmapBytes: slice of bytes representing valid or null values.
type Buffer struct {
	Data            interface{}
	nullBitmapBytes []byte
}

func buildNullBitmapBytes(dataLength int, validityArray interface{}) []byte {
	var res []byte
	nullBitmapLength := bitutil.CeilByte(dataLength) / 8

	switch valid := validityArray.(type) {
	case nil:
		res = make([]byte, nullBitmapLength)
		for i := 0; i < dataLength; i++ {
			bitutil.SetBit(res, i)
		}
	case []bool:
		if len(valid) != dataLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		res = make([]byte, nullBitmapLength)
		for i := 0; i < dataLength; i++ {
			if valid[i] {
				bitutil.SetBit(res, i)
			}
		}
	case []byte:
		if len(valid) != nullBitmapLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		return valid
	default:
		panic(fmt.Errorf("unsupported type %T", valid))
	}

	return res
}

// NewBufferFromInterfaces returns a new typed Buffer with the data represented as a slice of interface{}, with eventual nil values.
func NewBufferFromInterfaces(typ Type, data []interface{}) (Buffer, error) {
	buf := NewBuffer(len(data), typ)
	for i, c := range data {
		buf.SetOrDrop(i, c)
	}
	return buf, nil
}

// IsValid return true if the value at row `rowIndex` is valid.
func (b Buffer) IsValid(rowIndex int) bool {
	return bitutil.BitIsSet(b.nullBitmapBytes, rowIndex)
}

// IsNull return true if the value at row `rowIndex` is nil.
func (b Buffer) IsNull(rowIndex int) bool {
	return bitutil.BitIsNotSet(b.nullBitmapBytes, rowIndex)
}

// IsSorted returns true if the values of the Buffer are sorted in ascending order.
func (b Buffer) IsSorted() bool { return sort.IsSorted(b) }

// Swap swaps the values of the Buffer at indices i and j.
func (b Buffer) Swap(i, j int) {
	v1, v2 := b.GetValue(i), b.GetValue(j)
	b.SetOrDropStrict(i, v2)
	b.SetOrDropStrict(j, v1)
}
