package bow

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// Series is wrapping the Apache Arrow arrow.Array interface, with the addition of a name.
// It represents an immutable sequence of values using the Arrow in-memory format.
type Series struct {
	Name  string
	Array arrow.Array
}

// NewSeries returns a new Series from:
// - name: string
// - dataArray: slice of the data in any of the Bow supported types
// - validityArray:
//  - If nil, the data will be non-nil
//  - Can be of type []bool or []byte to represent nil values
func NewSeries(name string, dataArray interface{}, validityArray interface{}) Series {
	switch v := dataArray.(type) {
	case []int64:
		return newInt64Series(name, v, buildNullBitmapBytes(len(v), validityArray))
	case []float64:
		return newFloat64Series(name, v, buildNullBitmapBytes(len(v), validityArray))
	case []bool:
		return newBooleanSeries(name, v, buildNullBitmapBytes(len(v), validityArray))
	case []string:
		return newStringSeries(name, v, buildNullBitmapBytes(len(v), validityArray))
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}
}

// NewSeriesFromBuffer returns a new Series from a name and a Buffer.
func NewSeriesFromBuffer(name string, buf Buffer) Series {
	switch data := buf.Data.(type) {
	case []int64:
		return newInt64Series(name, data, buf.nullBitmapBytes)
	case []float64:
		return newFloat64Series(name, data, buf.nullBitmapBytes)
	case []bool:
		return newBooleanSeries(name, data, buf.nullBitmapBytes)
	case []string:
		return newStringSeries(name, data, buf.nullBitmapBytes)
	default:
		panic(fmt.Errorf("unsupported type '%T'", buf.Data))
	}
}

func newInt64Series(name string, data []int64, valid []byte) Series {
	length := len(data)
	return Series{
		Name: name,
		Array: array.NewInt64Data(
			array.NewData(mapBowToArrowTypes[Int64], length,
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
			array.NewData(mapBowToArrowTypes[Float64], length,
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

// NewSeriesFromInterfaces returns a new Series from:
// - name: string
// - typ: Bow Type
// - data: represented by a slice of interface{}, with eventually nil values
func NewSeriesFromInterfaces(name string, typ Type, data []interface{}) Series {
	if typ == Unknown {
		var err error
		if typ, err = getBowTypeFromInterfaces(data); err != nil {
			panic(err)
		}
	}
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch typ {
	case Int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.Resize(len(data))
		for i := 0; i < len(data); i++ {
			v, ok := ToInt64(data[i])
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
		builder.Resize(len(data))
		for i := 0; i < len(data); i++ {
			v, ok := ToFloat64(data[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	case Boolean:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.Resize(len(data))
		for i := 0; i < len(data); i++ {
			v, ok := ToBoolean(data[i])
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
		builder.Resize(len(data))
		for i := 0; i < len(data); i++ {
			v, ok := ToString(data[i])
			if !ok {
				builder.AppendNull()
				continue
			}
			builder.Append(v)
		}
		return Series{Name: name, Array: builder.NewArray()}
	default:
		panic(fmt.Errorf("unhandled type %s", typ))
	}
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
		panic(fmt.Errorf("unsupported type %T", valid))
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
				return Boolean, nil
			}
		}
	}

	return Float64, nil
}
