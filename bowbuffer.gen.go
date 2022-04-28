// Code generated by bowbuffer.gen.go.tmpl. DO NOT EDIT.

package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
)

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

// NewBufferFromData returns from `data`, which has to be a slice of a supported type.
func NewBufferFromData(data interface{}) Buffer {
	var l int
	switch data.(type) {
	case []int64:
	case []float64:
	case []bool:
	case []string:
	default:
		panic(fmt.Errorf("unhandled type %T", data))
	}
	return Buffer{
		Data:            data,
		nullBitmapBytes: buildNullBitmapBytes(l, nil),
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

// SetOrDrop sets the value `value` at index `i` by attempting a type assertion to the Buffer Type.
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
