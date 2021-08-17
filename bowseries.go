package bow

import (
	"encoding/json"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// A Series is simply a named Apache Arrow array.Interface, which is immutable
type Series struct {
	Name  string
	Array array.Interface
}

func NewSeries(name string, typ Type, dataArray interface{}, validArray []bool) Series {
	var newArray array.Interface
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch typ {
	case Float64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		b.AppendValues(dataArray.([]float64), validArray)
		newArray = b.NewArray()
	case Int64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendValues(dataArray.([]int64), validArray)
		newArray = b.NewArray()
	case Bool:
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		b.AppendValues(dataArray.([]bool), validArray)
		newArray = b.NewArray()
	case String:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		b.AppendValues(dataArray.([]string), validArray)
		newArray = b.NewArray()
	}
	return Series{
		Name:  name,
		Array: newArray,
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
	return NewSeries(name, typ, buf.Value, buf.Valid), nil
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
				return Bool, nil
			}
		}
	}
	return Float64, nil
}
