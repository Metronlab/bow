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
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	switch typ {
	case Float64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]float64), validArray)
		newArray = builder.NewArray()
	case Int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]int64), validArray)
		newArray = builder.NewArray()
	case Bool:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]bool), validArray)
		newArray = builder.NewArray()
	case String:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]string), validArray)
		newArray = builder.NewArray()
	}

	return Series{
		Name:  name,
		Array: newArray,
	}
}

func (b *bow) NewSeriesFromCol(colIndex int) Series {
	return Series{
		Name:  b.GetColName(colIndex),
		Array: b.Column(colIndex),
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
