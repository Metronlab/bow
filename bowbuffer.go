package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
)

type Buffer struct {
	Value interface{}
	Valid []bool
}

func NewBuffer(size int, typ Type, nullable bool) Buffer {
	var valid []bool
	if nullable {
		valid = make([]bool, size)
	}
	switch typ {
	case Int64:
		return Buffer{
			Value: make([]int64, size),
			Valid: valid,
		}
	case Float64:
		return Buffer{
			Value: make([]float64, size),
			Valid: valid,
		}
	case Bool:
		return Buffer{
			Value: make([]bool, size),
			Valid: valid,
		}
	case String:
		return Buffer{
			Value: make([]string, size),
			Valid: valid,
		}
	default:
		panic(fmt.Errorf("bow.NewBuffer: unsupported type %v", typ))
	}
}

func (b *bow) NewBufferFromCol(colIndex int) Buffer {
	colType := b.ColumnType(colIndex)
	colData := b.Column(colIndex).Data()
	switch colType {
	case Int64:
		colArray := array.NewInt64Data(colData)
		return Buffer{
			Value: colArray.Int64Values(),
			Valid: getValiditySlice(colArray),
		}
	case Float64:
		colArray := array.NewFloat64Data(colData)
		return Buffer{
			Value: colArray.Float64Values(),
			Valid: getValiditySlice(colArray),
		}
	case Bool:
		colArray := array.NewBooleanData(colData)
		var v = make([]bool, colArray.Len())
		for i := range v {
			v[i] = colArray.Value(i)
		}
		return Buffer{
			Value: v,
			Valid: getValiditySlice(colArray),
		}
	case String:
		colArray := array.NewStringData(colData)
		var v = make([]string, colArray.Len())
		for i := range v {
			v[i] = colArray.Value(i)
		}
		return Buffer{
			Value: v,
			Valid: getValiditySlice(colArray),
		}
	default:
		panic(fmt.Errorf("bow.NewBufferFromCol: unsupported type %+v", colType))
	}
}

func NewBufferFromInterfaces(typ Type, cells []interface{}) (Buffer, error) {
	buf := NewBuffer(len(cells), typ, true)
	for i, c := range cells {
		buf.SetOrDrop(i, c)
	}
	return buf, nil
}

func (b *Buffer) SetOrDrop(i int, value interface{}) {
	switch v := b.Value.(type) {
	case []int64:
		v[i], b.Valid[i] = Int64.Convert(value).(int64)
	case []float64:
		v[i], b.Valid[i] = Float64.Convert(value).(float64)
	case []bool:
		v[i], b.Valid[i] = Bool.Convert(value).(bool)
	case []string:
		v[i], b.Valid[i] = String.Convert(value).(string)
	default:
		panic(fmt.Errorf("bow.Buffer.SetOrDrop: unsupported type %T", v))
	}
}

func (b *Buffer) SetOrDropStrict(i int, value interface{}) {
	switch v := b.Value.(type) {
	case []int64:
		v[i], b.Valid[i] = value.(int64)
	case []float64:
		v[i], b.Valid[i] = value.(float64)
	case []bool:
		v[i], b.Valid[i] = value.(bool)
	case []string:
		v[i], b.Valid[i] = value.(string)
	default:
		panic(fmt.Errorf("unsupported type %T", v))
	}
}

func (b *Buffer) GetValue(i int) interface{} {
	switch v := b.Value.(type) {
	case []int64:
		if !b.Valid[i] {
			return nil
		}
		return v[i]
	case []float64:
		if !b.Valid[i] {
			return nil
		}
		return v[i]
	case []bool:
		if !b.Valid[i] {
			return nil
		}
		return v[i]
	case []string:
		if !b.Valid[i] {
			return nil
		}
		return v[i]
	default:
		panic(fmt.Errorf("bow.Buffer.GetValue: unsupported type %T", v))
	}
}

func (b *Buffer) SetAsValid(rowIndex int) {
	b.Valid[rowIndex] = true
}
