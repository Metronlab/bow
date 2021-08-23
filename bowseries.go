package bow

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow/array"
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

func NewSeriesFromData(name string, typ Type, dataArray interface{}, validityArray interface{}) Series {
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

func (s *Series) IsValid(rowIndex int) bool {
	return bitutil.BitIsSet(s.nullBitmapBytes, rowIndex)
}

func (s *Series) IsNull(rowIndex int) bool {
	return bitutil.BitIsNotSet(s.nullBitmapBytes, rowIndex)
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
