package bow

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// Series is wrapping the Apache Arrow arrow.Array interface, with the addition of a name.
// It represents an immutable sequence of values using the Arrow in-memory format.
type Series struct {
	Name  string
	Array arrow.Array
}

func NewSeries(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
	switch typ {
	case Int64:
		return newInt64Series(name, dataArray.([]int64),
			buildNullBitmapBytes(len(dataArray.([]int64)), validityArray))
	case Float64:
		return newFloat64Series(name, dataArray.([]float64),
			buildNullBitmapBytes(len(dataArray.([]float64)), validityArray))
	case Bool:
		return newBooleanSeries(name, dataArray.([]bool),
			buildNullBitmapBytes(len(dataArray.([]bool)), validityArray))
	case String:
		return newStringSeries(name, dataArray.([]string),
			buildNullBitmapBytes(len(dataArray.([]string)), validityArray))
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return newTimestampSeries(name, typ, dataArray.([]arrow.Timestamp),
			buildNullBitmapBytes(len(dataArray.([]arrow.Timestamp)), validityArray))
	default:
		panic(fmt.Errorf("unsupported type '%s'", typ))
	}
}

func NewSeriesFromBuffer(name string, buf Buffer) Series {
	switch buf.DataType {
	case Int64:
		return newInt64Series(name, buf.Data.([]int64), buf.nullBitmapBytes)
	case Float64:
		return newFloat64Series(name, buf.Data.([]float64), buf.nullBitmapBytes)
	case Bool:
		return newBooleanSeries(name, buf.Data.([]bool), buf.nullBitmapBytes)
	case String:
		return newStringSeries(name, buf.Data.([]string), buf.nullBitmapBytes)
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		return newTimestampSeries(name, buf.DataType, buf.Data.([]arrow.Timestamp), buf.nullBitmapBytes)
	default:
		panic(fmt.Errorf("unsupported type '%s'", buf.DataType))
	}
}

func NewSeriesFromInterfaces(name string, typ Type, cells []interface{}) Series {
	if typ == Unknown {
		var err error
		if typ, err = getBowTypeFromInterfaces(cells); err != nil {
			panic(err)
		}
	}

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch typ {
	case Int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.Resize(len(cells))
		for i := 0; i < len(cells); i++ {
			v, ok := ToInt64(cells[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	case Float64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		builder.Resize(len(cells))
		for i := 0; i < len(cells); i++ {
			v, ok := ToFloat64(cells[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	case Bool:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.Resize(len(cells))
		for i := 0; i < len(cells); i++ {
			v, ok := ToBool(cells[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	case String:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.Resize(len(cells))
		for i := 0; i < len(cells); i++ {
			v, ok := ToString(cells[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		builder := array.NewTimestampBuilder(mem, mapBowToArrowDataTypes[typ].(*arrow.TimestampType))
		defer builder.Release()
		builder.Resize(len(cells))
		for i := 0; i < len(cells); i++ {
			v, ok := mapBowTypeToConvertFunc[typ](cells[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	default:
		panic(fmt.Errorf("unsupported type '%s'", typ))
	}
}

func newInt64Series(name string, data []int64, valid []byte) Series {
	length := len(data)
	return Series{
		Name: name,
		Array: array.NewInt64Data(
			array.NewData(mapBowToArrowDataTypes[Int64], length,
				[]*memory.Buffer{
					memory.NewBufferBytes(valid),
					memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(data)),
				}, nil, length-bitutil.CountSetBits(valid, 0, length), 0),
		),
	}
}

func newFloat64Series(name string, data []float64, valid []byte) Series {
	length := len(data)
	return Series{
		Name: name,
		Array: array.NewFloat64Data(
			array.NewData(mapBowToArrowDataTypes[Float64], length,
				[]*memory.Buffer{
					memory.NewBufferBytes(valid),
					memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(data)),
				}, nil, length-bitutil.CountSetBits(valid, 0, length), 0),
		),
	}
}

func newBooleanSeries(name string, data []bool, valid []byte) Series {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data, buildNullBitmapBool(len(data), valid))
	return Series{Name: name, Array: builder.NewArray()}
}

func newStringSeries(name string, data []string, valid []byte) Series {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data, buildNullBitmapBool(len(data), valid))
	return Series{Name: name, Array: builder.NewArray()}
}

func newTimestampSeries(name string, typ Type, data []arrow.Timestamp, valid []byte) Series {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewTimestampBuilder(mem, mapBowToArrowDataTypes[typ].(*arrow.TimestampType))
	defer builder.Release()
	builder.AppendValues(data, buildNullBitmapBool(len(data), valid))
	return Series{Name: name, Array: builder.NewArray()}
}

func buildNullBitmapBool(dataLength int, validityArray interface{}) []bool {
	switch valid := validityArray.(type) {
	case nil:
		return nil
	case []bool:
		if len(valid) != dataLength {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		return valid
	case []byte:
		if len(valid) != bitutil.CeilByte(dataLength)/8 {
			panic(fmt.Errorf("dataArray and validityArray have different lengths"))
		}
		res := make([]bool, dataLength)
		for i := 0; i < dataLength; i++ {
			if bitutil.BitIsSet(valid, i) {
				res[i] = true
			}
		}
		return res
	default:
		panic(fmt.Errorf("unsupported type '%T'", valid))
	}
}

func getBowTypeFromInterfaces(colBasedData []interface{}) (Type, error) {
	for _, val := range colBasedData {
		if val != nil {
			switch val.(type) {
			case float64, json.Number:
				return Float64, nil
			case int, int64:
				return Int64, nil
			case string:
				return String, nil
			case bool:
				return Bool, nil
			}
		}
	}

	return Float64, nil
}
