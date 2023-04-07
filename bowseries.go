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
// - typ: Bow data Type
// - dataArray: slice of the data
// - validityArray:
//   - if nil, the data will be non-nil
//   - can be of type []bool or []byte to represent nil values
func NewSeries(name string, typ Type, dataArray, validityArray interface{}) Series {
	return newSeries(name, typ, dataArray, validityArray)
}

// NewSeriesFromBuffer returns a new Series from a name and a Buffer.
func NewSeriesFromBuffer(name string, buf Buffer) Series {
	return newSeries(name, buf.DataType, buf.Data, buf.nullBitmapBytes)
}

func newSeries(name string, typ Type, dataArray, validityArray interface{}) Series {
	switch typ {
	case Int64:
		length := len(dataArray.([]int64))
		nullBitmapBytes := buildNullBitmapBytes(length, validityArray)
		return newInt64Series(name, length, dataArray.([]int64), nullBitmapBytes)
	case Float64:
		length := len(dataArray.([]float64))
		nullBitmapBytes := buildNullBitmapBytes(length, validityArray)
		return newFloat64Series(name, length, dataArray.([]float64), nullBitmapBytes)
	case Boolean:
		length := len(dataArray.([]bool))
		nullBitmapBool := buildNullBitmapBool(length, validityArray)
		return newBooleanSeries(name, dataArray.([]bool), nullBitmapBool)
	case String:
		length := len(dataArray.([]string))
		nullBitmapBool := buildNullBitmapBool(length, validityArray)
		return newStringSeries(name, dataArray.([]string), nullBitmapBool)
	default:
		panic(fmt.Errorf("unsupported type '%s'", typ))
	}
}

func newInt64Series(name string, length int, data []int64, valid []byte) Series {
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

func newFloat64Series(name string, length int, data []float64, valid []byte) Series {
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

func newBooleanSeries(name string, data []bool, valid []bool) Series {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data, valid)
	return Series{Name: name, Array: builder.NewArray()}
}

func newStringSeries(name string, data []string, valid []bool) Series {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues(data, valid)
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
		panic(fmt.Errorf("unsupported type '%s'", typ))
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
		panic(fmt.Errorf("unsupported type '%T'", valid))
	}

	return res
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
