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
// - dataArray: slice of the data (for Timestamp types, data can be of type []int64 or []arrow.Timestamp)
// - validityArray:
//  - if nil, the data will be non-nil
//  - can be of type []bool or []byte to represent nil values
func NewSeries(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
	switch typ {
	case Int64:
		length := len(dataArray.([]int64))
		valid := buildNullBitmapBytes(length, validityArray)
		return Series{
			Name: name,
			Array: array.NewInt64Data(
				array.NewData(mapBowToArrowTypes[Int64], length,
					[]*memory.Buffer{
						memory.NewBufferBytes(valid),
						memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(dataArray.([]int64))),
					}, nil, length-bitutil.CountSetBits(valid, 0, length), 0),
			),
		}
	case Float64:
		length := len(dataArray.([]float64))
		valid := buildNullBitmapBytes(length, validityArray)
		return Series{
			Name: name,
			Array: array.NewFloat64Data(
				array.NewData(mapBowToArrowTypes[Float64], length,
					[]*memory.Buffer{
						memory.NewBufferBytes(valid),
						memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(dataArray.([]float64))),
					}, nil, length-bitutil.CountSetBits(valid, 0, length), 0),
			),
		}
	case Boolean:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]bool), buildNullBitmapBool(len(dataArray.([]bool)), validityArray))
		return Series{Name: name, Array: builder.NewArray()}
	case String:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]string), buildNullBitmapBool(len(dataArray.([]string)), validityArray))
		return Series{Name: name, Array: builder.NewArray()}
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewTimestampBuilder(mem, mapBowToArrowTypes[typ].(*arrow.TimestampType))
		defer builder.Release()
		switch data := dataArray.(type) {
		case []arrow.Timestamp:
			builder.AppendValues(data, buildNullBitmapBool(len(data), validityArray))
		case []int64:
			tsData := make([]arrow.Timestamp, len(data))
			for i, intVal := range data {
				tsData[i] = arrow.Timestamp(intVal)
			}
			builder.AppendValues(tsData, buildNullBitmapBool(len(tsData), validityArray))
		default:
			panic(fmt.Errorf("unsupported type '%T' for Timestamp dataArray", dataArray))
		}
		return Series{Name: name, Array: builder.NewArray()}
	default:
		panic(fmt.Errorf("unsupported type '%s'", typ))
	}
}

// NewSeriesFromBuffer returns a new Series from a name and a Buffer.
func NewSeriesFromBuffer(name string, buf Buffer) Series {
	switch buf.DataType {
	case Int64:
		length := len(buf.Data.([]int64))
		return Series{
			Name: name,
			Array: array.NewInt64Data(
				array.NewData(mapBowToArrowTypes[Int64], length,
					[]*memory.Buffer{
						memory.NewBufferBytes(buf.nullBitmapBytes),
						memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(buf.Data.([]int64))),
					}, nil, length-bitutil.CountSetBits(buf.nullBitmapBytes, 0, length), 0),
			),
		}
	case Float64:
		length := len(buf.Data.([]float64))
		return Series{
			Name: name,
			Array: array.NewFloat64Data(
				array.NewData(mapBowToArrowTypes[Float64], length,
					[]*memory.Buffer{
						memory.NewBufferBytes(buf.nullBitmapBytes),
						memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(buf.Data.([]float64))),
					}, nil, length-bitutil.CountSetBits(buf.nullBitmapBytes, 0, length), 0),
			),
		}
	case Boolean:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.AppendValues(buf.Data.([]bool), buildNullBitmapBool(len(buf.Data.([]bool)), buf.nullBitmapBytes))
		return Series{Name: name, Array: builder.NewArray()}
	case String:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(buf.Data.([]string), buildNullBitmapBool(len(buf.Data.([]string)), buf.nullBitmapBytes))
		return Series{Name: name, Array: builder.NewArray()}
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewTimestampBuilder(mem, mapBowToArrowTypes[buf.DataType].(*arrow.TimestampType))
		defer builder.Release()
		switch data := buf.Data.(type) {
		case []arrow.Timestamp:
			builder.AppendValues(data, buildNullBitmapBool(len(data), buf.nullBitmapBytes))
		case []int64:
			tsData := make([]arrow.Timestamp, len(data))
			for i, intVal := range data {
				tsData[i] = arrow.Timestamp(intVal)
			}
			builder.AppendValues(tsData, buildNullBitmapBool(len(tsData), buf.nullBitmapBytes))
		default:
			panic(fmt.Errorf("unsupported type '%T' for Buffer Data", buf.Data))
		}
		return Series{Name: name, Array: builder.NewArray()}
	default:
		panic(fmt.Errorf("unsupported type '%s'", buf.DataType))
	}
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
	case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
		builder := array.NewTimestampBuilder(mem, mapBowToArrowTypes[typ].(*arrow.TimestampType))
		defer builder.Release()
		builder.Resize(len(data))
		for i := 0; i < len(data); i++ {
			v, ok := ToTimestamp(data[i], mapBowTypeToTimeUnit[typ])
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
