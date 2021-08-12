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

func NewSeries(name string, typ Type, dataArray interface{}, validArray []bool) Series {
	switch typ {
	case Float64:
		typedDataArray, ok := dataArray.([]float64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v", typ, reflect.TypeOf(dataArray)))
		}
		length := len(typedDataArray)
		valid := setValid(validArray, length)
		return Series{
			Name: name,
			Array: array.NewFloat64Data(
				array.NewData(arrow.PrimitiveTypes.Float64, length,
					[]*memory.Buffer{
						memory.NewBufferBytes(valid),
						memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(typedDataArray)),
					}, nil, length-bitutil.CountSetBits(valid, 0, length), 0),
			),
		}
	case Int64:
		typedDataArray, ok := dataArray.([]int64)
		if !ok {
			panic(fmt.Errorf(
				"bow.NewSeries: typ is %v, but have %v", typ, reflect.TypeOf(dataArray)))
		}
		length := len(typedDataArray)
		valid := setValid(validArray, length)
		return Series{
			Name: name,
			Array: array.NewInt64Data(
				array.NewData(arrow.PrimitiveTypes.Int64, length,
					[]*memory.Buffer{
						memory.NewBufferBytes(valid),
						memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(typedDataArray)),
					}, nil, length-bitutil.CountSetBits(valid, 0, length), 0),
			),
		}
	case Bool:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]bool), validArray)
		return Series{Name: name, Array: builder.NewArray()}
	case String:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(dataArray.([]string), validArray)
		return Series{Name: name, Array: builder.NewArray()}
	default:
		panic(fmt.Errorf("bow.NewSeries: unsupported type %v", typ))
	}
}

func setValid(validArray []bool, length int) []byte {
	var valid = make([]byte, length)
	if len(validArray) == 0 {
		for i := range valid {
			bitutil.SetBit(valid, i)
		}
	} else if length == len(validArray) {
		for i, vd := range validArray {
			if vd == true {
				bitutil.SetBit(valid, i)
			}
		}
	} else {
		panic(fmt.Errorf(
			"bow.NewSeries: dataArray and validArray lengths don't match"))
	}

	return valid
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
