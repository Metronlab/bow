// Code generated by bowseries.gen.go.tmpl. DO NOT EDIT.

package bow

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

// A Series is simply a named Apache Arrow array.Interface, which is immutable
type Series struct {
	Name  string
	Array array.Interface
}

func NewSeries(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
	switch typ {
	case Int64:
		data, ok := dataArray.([]int64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newInt64Series(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	case Float64:
		data, ok := dataArray.([]float64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newFloat64Series(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	case Boolean:
		data, ok := dataArray.([]bool)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newBooleanSeries(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	case String:
		data, ok := dataArray.([]string)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newStringSeries(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	default:
		panic(fmt.Errorf("unsupported type %v", typ))
	}
}

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
			array.NewData(arrow.PrimitiveTypes.Int64, length,
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
			array.NewData(arrow.PrimitiveTypes.Float64, length,
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
	builder.AppendValues(data,
		buildNullBitmapBool(len(data), valid))
	return Series{Name: name, Array: builder.NewArray()}
}

func newStringSeries(name string, data []string, valid []byte) Series {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data,
		buildNullBitmapBool(len(data), valid))
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
		panic(fmt.Errorf("unsupported type %T", valid))
	}
}

func NewSeriesFromInterfaces(name string, typ Type, cells []interface{}) (series Series, err error) {
	if typ == Unknown {
		if typ, err = seekType(cells); err != nil {
			return
		}
	}

	buf, err := NewBufferFromInterfaces(typ, cells)
	if err != nil {
		return Series{}, err
	}

	return NewSeries(name, typ, buf.Data, buf.nullBitmapBytes), nil
}

func seekType(cells []interface{}) (Type, error) {
	for _, val := range cells {
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
