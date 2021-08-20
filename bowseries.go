package bow

import (
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"reflect"

	"github.com/apache/arrow/go/arrow/bitutil"
)

type Series struct {
	Name            string
	Data            interface{}
	nullBitmapBytes []byte
}

func NewSeries(name string, size int, typ Type) Series {
	switch typ {
	case Int64:
		return Series{
			Name:            name,
			Data:            make([]int64, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case Float64:
		return Series{
			Name:            name,
			Data:            make([]float64, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case Boolean:
		return Series{
			Name:            name,
			Data:            make([]bool, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	case String:
		return Series{
			Name:            name,
			Data:            make([]string, size),
			nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
		}
	default:
		panic(fmt.Errorf("unsupported type %s", typ))
	}
}

func (b *bow) NewSeriesFromCol(colIndex int) Series {
	data := b.Column(colIndex).Data()
	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Series{
			Name:            b.ColumnName(colIndex),
			Data:            Int64Values(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	case Float64:
		arr := array.NewFloat64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Series{
			Name:            b.ColumnName(colIndex),
			Data:            Float64Values(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	case Boolean:
		arr := array.NewBooleanData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Series{
			Name:            b.ColumnName(colIndex),
			Data:            BooleanValues(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	case String:
		arr := array.NewStringData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		return Series{
			Name:            b.ColumnName(colIndex),
			Data:            StringValues(arr),
			nullBitmapBytes: nullBitmapBytesCopy,
		}
	default:
		panic(fmt.Errorf(
			"unsupported type %+v", b.ColumnType(colIndex)))
	}
}

func (b *Series) Len() int {
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

func (b *Series) SetOrDrop(i int, value interface{}) {
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

func (b *Series) SetOrDropStrict(i int, value interface{}) {
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

func (b *Series) GetValue(i int) interface{} {
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

func (b *Series) IsValid(rowIndex int) bool {
	return bitutil.BitIsSet(b.nullBitmapBytes, rowIndex)
}

func (b *Series) IsNull(rowIndex int) bool {
	return bitutil.BitIsNotSet(b.nullBitmapBytes, rowIndex)
}

func NewSeriesFromData(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
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

func NewSeriesFromInterfaces(name string, typ Type, cells []interface{}) (Series, error) {
	var err error
	if typ == Unknown {
		if typ, err = seekType(cells); err != nil {
			return Series{}, err
		}
	}

	series := NewSeries(name, len(cells), typ)
	for i, c := range cells {
		series.SetOrDrop(i, c)
	}

	return series, nil
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
