package bow

import (
	"fmt"
)

type Buffer struct {
	Value interface{}
	Valid []bool
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

func (b *Buffer) GetPreviousValue(rowIndex int) (interface{}, int) {
	switch v := b.Value.(type) {
	case []int64:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex--
		}
		return int64(0), -1
	case []float64:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex--
		}
		return float64(0), -1
	case []bool:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex--
		}
		return false, -1
	case []string:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex--
		}
		return "", -1
	default:
		panic(fmt.Errorf("bow.Buffer.GetPreviousValue: unsupported type %T", v))
	}
}

func (b *Buffer) GetNextValue(rowIndex int) (interface{}, int) {
	switch v := b.Value.(type) {
	case []int64:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex++
		}
		return int64(0), -1
	case []float64:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex++
		}
		return float64(0), -1
	case []bool:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex++
		}
		return false, -1
	case []string:
		for rowIndex >= 0 && rowIndex < len(v) {
			if b.Valid[rowIndex] {
				return v[rowIndex], rowIndex
			}
			rowIndex++
		}
		return "", -1
	default:
		panic(fmt.Errorf("bow.Buffer.GetNextValue: unsupported type %T", v))
	}
}

func (b *Buffer) GetFloat64(rowIndex int) (float64, bool) {
	switch v := b.Value.(type) {
	case []int64:
		if rowIndex < 0 || rowIndex >= len(v) {
			return float64(0), false
		}
		return float64(v[rowIndex]), b.Valid[rowIndex]
	case []float64:
		if rowIndex < 0 || rowIndex >= len(v) {
			return float64(0), false
		}
		return v[rowIndex], b.Valid[rowIndex]
	case []bool:
		if rowIndex < 0 || rowIndex >= len(v) {
			return float64(0), false
		}
		if v[rowIndex] {
			return float64(1), b.Valid[rowIndex]
		}
		return float64(0), b.Valid[rowIndex]
	case []string:
		if rowIndex < 0 || rowIndex >= len(v) {
			return float64(0), false
		}
		val, ok := ToFloat64(v[rowIndex])
		if !ok {
			return float64(0), false
		}
		return val, b.Valid[rowIndex]
	default:
		panic(fmt.Errorf("bow.Buffer.GetFloat64: unsupported type %T", v))
	}
}
