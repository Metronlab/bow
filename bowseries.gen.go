// Code generated by bowseries.gen.go.tmpl. DO NOT EDIT.

package bow

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
)

func NewSeries(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
	switch typ {
	case Int64:
		data, ok := dataArray.([]int64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return Series{
			Name:            name,
			Data:            data,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	case Float64:
		data, ok := dataArray.([]float64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return Series{
			Name:            name,
			Data:            data,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	case Boolean:
		data, ok := dataArray.([]bool)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return Series{
			Name:            name,
			Data:            data,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	case String:
		data, ok := dataArray.([]string)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v",
				typ, reflect.TypeOf(dataArray)))
		}
		return Series{
			Name:            name,
			Data:            data,
			nullBitmapBytes: buildNullBitmapBytes(len(data), validityArray),
		}
	default:
		panic(fmt.Errorf("unsupported type %v", typ))
	}
}

func NewSeriesEmpty(name string, size int, typ Type) Series {
	var res = Series{
		Name:            name,
		nullBitmapBytes: make([]byte, bitutil.CeilByte(size)/8),
	}

	switch typ {
	case Int64:
		res.Data = make([]int64, size)
	case Float64:
		res.Data = make([]float64, size)
	case Boolean:
		res.Data = make([]bool, size)
	case String:
		res.Data = make([]string, size)
	default:
		panic(fmt.Errorf("unsupported type %s", typ))
	}

	return res
}

func (b *bow) NewSeriesFromCol(colIndex int) Series {
	var res = Series{
		Name: b.ColumnName(colIndex),
	}

	data := b.Column(colIndex).Data()
	switch b.ColumnType(colIndex) {
	case Int64:
		arr := array.NewInt64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = Int64Values(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case Float64:
		arr := array.NewFloat64Data(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = Float64Values(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case Boolean:
		arr := array.NewBooleanData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = BooleanValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	case String:
		arr := array.NewStringData(data)
		nullBitmapBytes := arr.NullBitmapBytes()[:bitutil.CeilByte(arr.Data().Len())/8]
		nullBitmapBytesCopy := make([]byte, len(nullBitmapBytes))
		copy(nullBitmapBytesCopy, nullBitmapBytes)
		res.Data = StringValues(arr)
		res.nullBitmapBytes = nullBitmapBytesCopy
	default:
		panic(fmt.Errorf(
			"unsupported type %+v", b.ColumnType(colIndex)))
	}

	return res
}

func (s *Series) Len() int {
	switch data := s.Data.(type) {
	case []int64:
		return len(data)
	case []float64:
		return len(data)
	case []bool:
		return len(data)
	case []string:
		return len(data)
	default:
		panic(fmt.Errorf("unsupported type '%T'", s.Data))
	}
}

func (s *Series) DataType() Type {
	switch s.Data.(type) {
	case []int64:
		return Int64
	case []float64:
		return Float64
	case []bool:
		return Boolean
	case []string:
		return String
	default:
		panic(fmt.Errorf("unsupported type '%T'", s.Data))
	}
}

func (s *Series) SetOrDrop(i int, value interface{}) {
	var valid bool
	switch v := s.Data.(type) {
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
		bitutil.SetBit(s.nullBitmapBytes, i)
	} else {
		bitutil.ClearBit(s.nullBitmapBytes, i)
	}
}

func (s *Series) SetOrDropStrict(i int, value interface{}) {
	var valid bool
	switch v := s.Data.(type) {
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
		bitutil.SetBit(s.nullBitmapBytes, i)
	} else {
		bitutil.ClearBit(s.nullBitmapBytes, i)
	}
}

func (s *Series) GetValue(i int) interface{} {
	if bitutil.BitIsNotSet(s.nullBitmapBytes, i) {
		return nil
	}

	switch v := s.Data.(type) {
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
