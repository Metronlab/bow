package bow

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// A Series is simply a named Apache Arrow array.Interface, which is immutable
type Series struct {
	Name  string
	Array array.Interface
}

func NewSeries(name string, t Type, dataArray interface{}, validArray []bool) Series {
	var newArray array.Interface
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch t {
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

func NewSeriesFromInterfaces(name string, typeOf Type, cells []interface{}) (series Series, err error) {
	if typeOf == Unknown {
		if typeOf, err = seekType(cells); err != nil {
			return
		}
	}
	buf, err := NewBufferFromInterfaces(typeOf, cells)
	if err != nil {
		return Series{}, err
	}
	return NewSeries(name, typeOf, buf.Value, buf.Valid), nil
}
