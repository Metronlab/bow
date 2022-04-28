package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
)

// Buffer is a mutable data structure with the purpose of easily building data Series with:
// - Data: slice of data.
// - nullBitmapBytes: slice of bytes representing valid or null values.
type Buffer struct {
	Data            interface{}
	nullBitmapBytes []byte
}

// NewBuffer returns a new Buffer of size `size` and Type `typ`.
func NewBuffer(size int, typ Type) Buffer {
	switch typ {
	case Int64:
		return Buffer{
			Data:            make([]int64, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case Float64:
		return Buffer{
			Data:            make([]float64, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case Boolean:
		return Buffer{
			Data:            make([]bool, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case String:
		return Buffer{
			Data:            make([]string, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	default:
		panic(fmt.Errorf("unsupported type %s", typ))
	}
}

// Len returns the length of the Buffer
func (b Buffer) Len() int {
	switch data := b.Data.(type) {
	case []int64:
		return len(data)
	case []float64:
		return len(data)
	case []bool:
		return len(data)
	case []string:
		return len(data)
	default:
		panic(fmt.Errorf("unsupported type '%T'", b.Data))
	}
}

// SetOrDrop sets the value `value` at index `i` by attempting a type conversion to the Buffer Type.
// Set the bit in the Buffer nullBitmapBytes if the conversion succeeded, or clear it otherwise.
func (b *Buffer) SetOrDrop(i int, value interface{}) {
	var valid bool
	switch v := b.Data.(type) {
	case []int64:
		v[i], valid = Int64.Convert(value).(int64)
	case []float64:
		v[i], valid = Float64.Convert(value).(float64)
	case []bool:
		v[i], valid = Boolean.Convert(value).(bool)
	case []string:
		v[i], valid = String.Convert(value).(string)
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}

	if valid {
		bitutil.SetBit(b.nullBitmapBytes, i)
	} else {
		bitutil.ClearBit(b.nullBitmapBytes, i)
	}
}

// SetOrDropStrict sets the value `value` at index `i` by attempting a type assertion to the Buffer Type.
// Set the bit in the Buffer nullBitmapBytes if the type assertion succeeded, or clear it otherwise.
func (b *Buffer) SetOrDropStrict(i int, value interface{}) {
	var valid bool
	switch v := b.Data.(type) {
	case []int64:
		v[i], valid = value.(int64)
	case []float64:
		v[i], valid = value.(float64)
	case []bool:
		v[i], valid = value.(bool)
	case []string:
		v[i], valid = value.(string)
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}

	if valid {
		bitutil.SetBit(b.nullBitmapBytes, i)
	} else {
		bitutil.ClearBit(b.nullBitmapBytes, i)
	}
}

// GetValue gets the value at index `i` from the Buffer
func (b *Buffer) GetValue(i int) interface{} {
	if bitutil.BitIsNotSet(b.nullBitmapBytes, i) {
		return nil
	}
	switch v := b.Data.(type) {
	case []int64:
		return v[i]
	case []float64:
		return v[i]
	case []bool:
		return v[i]
	case []string:
		return v[i]
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}
}

// Less returns whether the value at index `i` is less that the value at index `j`.
func (b Buffer) Less(i, j int) bool {
	switch v := b.Data.(type) {
	case []int64:
		return v[i] < v[j]
	case []float64:
		return v[i] < v[j]
	case []string:
		return v[i] < v[j]
	case []bool:
		return !v[i] && v[j]
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}
}

// NewBufferFromCol returns a new Buffer created from the column at index `colIndex`.
func (b *bow) NewBufferFromCol(colIndex int) Buffer {
	data := b.Column(colIndex).Data()
	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Buffer{
			Data:            Int64Values(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	case Float64:
		arr := array.NewFloat64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Buffer{
			Data:            Float64Values(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	case Boolean:
		arr := array.NewBooleanData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Buffer{
			Data:            BooleanValues(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	case String:
		arr := array.NewStringData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Buffer{
			Data:            StringValues(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	default:
		panic(fmt.Errorf(
			"unsupported type %+v", b.ColumnType(colIndex)))
	}
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
