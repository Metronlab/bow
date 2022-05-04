package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
)

// Buffer is a mutable data structure with the purpose of easily building data Series with:
// - Data: slice of data.
// - DataType: type of the data.
// - nullBitmapBytes: slice of bytes representing valid or null values.
type Buffer struct {
	Data            interface{}
	DataType        Type
	nullBitmapBytes []byte
}

// NewBuffer returns a new Buffer of size `size` and Type `typ`.
func NewBuffer(size int, typ Type) Buffer {
	buf := Buffer{
		DataType:        typ,
		nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
	}
	switch typ {
	case Int64:
		buf.Data = make([]int64, size)
	case Float64:
		buf.Data = make([]float64, size)
	case Boolean:
		buf.Data = make([]bool, size)
	case String:
		buf.Data = make([]string, size)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		buf.Data = make([]arrow.Timestamp, size)
	default:
		panic(fmt.Errorf("unsupported type '%s'", typ))
	}
	return buf
}

// Len returns the size of the underlying slice of data in the Buffer.
func (b Buffer) Len() int {
	switch b.DataType {
	case Int64:
		return len(b.Data.([]int64))
	case Float64:
		return len(b.Data.([]float64))
	case Boolean:
		return len(b.Data.([]bool))
	case String:
		return len(b.Data.([]string))
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return len(b.Data.([]arrow.Timestamp))
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}
}

// SetOrDrop sets the Buffer data at index `i` by attempting to convert `value` to its DataType.
// Sets the value to nil if the conversion failed or if `value` is nil.
func (b *Buffer) SetOrDrop(i int, value interface{}) {
	var valid bool
	switch b.DataType {
	case Int64:
		b.Data.([]int64)[i], valid = Int64.Convert(value).(int64)
	case Float64:
		b.Data.([]float64)[i], valid = Float64.Convert(value).(float64)
	case Boolean:
		b.Data.([]bool)[i], valid = Boolean.Convert(value).(bool)
	case String:
		b.Data.([]string)[i], valid = String.Convert(value).(string)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		b.Data.([]arrow.Timestamp)[i], valid = b.DataType.Convert(value).(arrow.Timestamp)
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}

	if valid {
		bitutil.SetBit(b.nullBitmapBytes, i)
	} else {
		bitutil.ClearBit(b.nullBitmapBytes, i)
	}
}

// SetOrDropStrict sets the Buffer data at index `i` by attempting a type assertion of `value` to its DataType.
// Sets the value to nil if the assertion failed or if `value` is nil.
func (b *Buffer) SetOrDropStrict(i int, value interface{}) {
	var valid bool
	switch b.DataType {
	case Int64:
		b.Data.([]int64)[i], valid = value.(int64)
	case Float64:
		b.Data.([]float64)[i], valid = value.(float64)
	case Boolean:
		b.Data.([]bool)[i], valid = value.(bool)
	case String:
		b.Data.([]string)[i], valid = value.(string)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		valid = true
		switch value := value.(type) {
		case arrow.Timestamp:
			b.Data.([]arrow.Timestamp)[i] = value
		case int64:
			b.Data.([]int64)[i] = value
		default:
			valid = false
		}
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}

	if valid {
		bitutil.SetBit(b.nullBitmapBytes, i)
	} else {
		bitutil.ClearBit(b.nullBitmapBytes, i)
	}
}

func (b *Buffer) GetValue(i int) interface{} {
	if bitutil.BitIsNotSet(b.nullBitmapBytes, i) {
		return nil
	}

	switch b.DataType {
	case Int64:
		return b.Data.([]int64)[i]
	case Float64:
		return b.Data.([]float64)[i]
	case Boolean:
		return b.Data.([]bool)[i]
	case String:
		return b.Data.([]string)[i]
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return b.Data.([]arrow.Timestamp)[i]
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}
}

func (b Buffer) Less(i, j int) bool {
	switch b.DataType {
	case Int64:
		return b.Data.([]int64)[i] < b.Data.([]int64)[j]
	case Float64:
		return b.Data.([]float64)[i] < b.Data.([]float64)[j]
	case String:
		return b.Data.([]string)[i] < b.Data.([]string)[j]
	case Boolean:
		return !b.Data.([]bool)[i] && b.Data.([]bool)[j]
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return b.Data.([]arrow.Timestamp)[i] < b.Data.([]arrow.Timestamp)[j]
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}
}

func (b *bow) NewBufferFromCol(colIndex int) Buffer {
	data := b.Column(colIndex).Data()
	res := Buffer{DataType: b.ColumnType(colIndex)}
	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = int64Values(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case Float64:
		arr := array.NewFloat64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = float64Values(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case Boolean:
		arr := array.NewBooleanData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = booleanValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case String:
		arr := array.NewStringData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = stringValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		arr := array.NewTimestampData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = timestampValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.ColumnType(colIndex)))
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
