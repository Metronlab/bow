package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
)

type Buffer struct {
	Data            interface{}
	DataType        Type
	nullBitmapBytes []byte
}

func NewBuffer(size int, typ Type) Buffer {
	res := Buffer{
		DataType:        typ,
		nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
	}

	switch typ {
	case Int64:
		res.Data = make([]int64, size)
	case Float64:
		res.Data = make([]float64, size)
	case Bool:
		res.Data = make([]bool, size)
	case String:
		res.Data = make([]string, size)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		res.Data = make([]arrow.Timestamp, size)
	default:
		panic(fmt.Errorf("unsupported type '%s'", typ))
	}
	return res
}

func (b Buffer) Len() int {
	switch b.DataType {
	case Int64:
		return len(b.Data.([]int64))
	case Float64:
		return len(b.Data.([]float64))
	case Bool:
		return len(b.Data.([]bool))
	case String:
		return len(b.Data.([]string))
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return len(b.Data.([]arrow.Timestamp))
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}
}

func (b *Buffer) SetOrDrop(i int, value interface{}) {
	var valid bool
	switch b.DataType {
	case Int64:
		b.Data.([]int64)[i], valid = Int64.Convert(value).(int64)
	case Float64:
		b.Data.([]float64)[i], valid = Float64.Convert(value).(float64)
	case Bool:
		b.Data.([]bool)[i], valid = Bool.Convert(value).(bool)
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

func (b *Buffer) SetOrDropStrict(i int, value interface{}) {
	var valid bool
	switch b.DataType {
	case Int64:
		b.Data.([]int64)[i], valid = value.(int64)
	case Float64:
		b.Data.([]float64)[i], valid = value.(float64)
	case Bool:
		b.Data.([]bool)[i], valid = value.(bool)
	case String:
		b.Data.([]string)[i], valid = value.(string)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		b.Data.([]arrow.Timestamp)[i], valid = value.(arrow.Timestamp)
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
	case Bool:
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
	case Bool:
		return !b.Data.([]bool)[i] && b.Data.([]bool)[j]
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return b.Data.([]arrow.Timestamp)[i] < b.Data.([]arrow.Timestamp)[j]
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.DataType))
	}
}

func (b *bow) NewBufferFromCol(colIndex int) Buffer {
	res := Buffer{DataType: b.ColumnType(colIndex)}
	arrayData := b.Column(colIndex).Data()
	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(arrayData)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = int64Values(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case Float64:
		arr := array.NewFloat64Data(arrayData)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = float64Values(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case Bool:
		arr := array.NewBooleanData(arrayData)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = booleanValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case String:
		arr := array.NewStringData(arrayData)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = stringValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		arr := array.NewTimestampData(arrayData)
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

func NewBufferFromInterfaces(typ Type, cells []interface{}) (Buffer, error) {
	buf := NewBuffer(len(cells), typ)
	for i, c := range cells {
		buf.SetOrDrop(i, c)
	}
	return buf, nil
}

func (b Buffer) IsValid(rowIndex int) bool {
	return bitutil.BitIsSet(b.nullBitmapBytes, rowIndex)
}

func (b Buffer) IsNull(rowIndex int) bool {
	return bitutil.BitIsNotSet(b.nullBitmapBytes, rowIndex)
}

func (b Buffer) IsSorted() bool { return sort.IsSorted(b) }

func (b Buffer) Swap(i, j int) {
	v1, v2 := b.GetValue(i), b.GetValue(j)
	b.SetOrDropStrict(i, v2)
	b.SetOrDropStrict(j, v1)
}
