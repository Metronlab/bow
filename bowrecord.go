package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

func newRecord(metadata Metadata, seriesSlice ...Series) (array.Record, error) {
	var fields []arrow.Field
	var arrays []array.Interface
	var nRows int64

	if len(seriesSlice) != 0 && seriesSlice[0].Array != nil {
		nRows = int64(seriesSlice[0].Array.Len())
	}

	for _, s := range seriesSlice {
		if s.Array == nil {
			return nil, errors.New("empty PrevSeries")
		}
		if s.Name == "" {
			return nil, errors.New("empty PrevSeries name")
		}
		if getBowTypeFromArrowType(s.Array.DataType()) == Unknown {
			return nil, fmt.Errorf("unsupported type: %s", s.Array.DataType().Name())
		}
		if int64(s.Array.Len()) != nRows {
			return nil,
				fmt.Errorf(
					"bow.PrevSeries '%s' has a length of %d, which is different from the previous ones",
					s.Name, s.Array.Len())
		}
		fields = append(fields, arrow.Field{Name: s.Name, Type: s.Array.DataType()})
		arrays = append(arrays, s.Array)
	}

	return array.NewRecord(
		arrow.NewSchema(fields, &metadata.Metadata),
		arrays, nRows), nil
}

func newSeriesFromData(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
	switch typ {
	case Int64:
		data, ok := dataArray.([]int64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeriesFromData: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newInt64Series(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	case Float64:
		data, ok := dataArray.([]float64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeriesFromData: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newFloat64Series(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	case Boolean:
		data, ok := dataArray.([]bool)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeriesFromData: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newBooleanSeries(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	case String:
		data, ok := dataArray.([]string)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeriesFromData: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return newStringSeries(name, data,
			buildNullBitmapBytes(len(data), validityArray))
	default:
		panic(fmt.Errorf("unsupported type %v", typ))
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
