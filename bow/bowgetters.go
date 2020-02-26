package bow

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

func (b *bow) GetRow(rowIndex int) map[string]interface{} {
	m := map[string]interface{}{}
	for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
		val := b.GetValue(colIndex, rowIndex)
		if val == nil {
			continue
		}
		m[b.Schema().Field(colIndex).Name] = val
	}
	return m
}

func (b *bow) GetValueByName(colName string, rowIndex int) interface{} {
	for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
		name := b.Schema().Field(colIndex).Name
		if colName == name {
			return b.GetValue(colIndex, rowIndex)
		}
	}
	return nil
}

func (b *bow) GetValue(colIndex, rowIndex int) interface{} {
	switch b.GetType(colIndex) {
	case Float64:
		vd := array.NewFloat64Data(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	case Int64:
		vd := array.NewInt64Data(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	case Bool:
		vd := array.NewBooleanData(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	case String:
		vd := array.NewStringData(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return vd.Value(rowIndex)
		}
	default:
		panic(fmt.Errorf("bow: unhandled type %s", b.GetType(colIndex)))
	}
	return nil
}

func (b *bow) GetNextValues(col1, col2, row int) (interface{}, interface{}, int) {
	if row < 0 || row >= b.NumRows() {
		return nil, nil, -1
	}

	for row >= 0 && row < b.NumRows() {
		var v1 interface{}
		v1, row = b.GetNextValue(col1, row)
		v2, row2 := b.GetNextValue(col2, row)
		if row == row2 {
			return v1, v2, row
		}

		row++
	}

	return nil, nil, -1
}

func (b *bow) GetNextValue(col, row int) (interface{}, int) {
	if row < 0 || row >= b.NumRows() {
		return nil, -1
	}

	for row < b.NumRows() {
		value := b.GetValue(col, row)
		if value != nil {
			return value, row
		}
		row++
	}
	return nil, -1
}

func (b *bow) GetPreviousValues(col1, col2, row int) (interface{}, interface{}, int) {
	if row < 0 || row >= b.NumRows() {
		return nil, nil, -1
	}

	for row >= 0 && row < b.NumRows() {
		var v1 interface{}
		v1, row = b.GetPreviousValue(col1, row)
		v2, row2 := b.GetPreviousValue(col2, row)
		if row == row2 {
			return v1, v2, row
		}
		row--
	}

	return nil, nil, -1
}

func (b *bow) GetPreviousValue(col, row int) (interface{}, int) {
	if row < 0 || row >= b.NumRows() {
		return nil, -1
	}

	for row >= 0 {
		value := b.GetValue(col, row)
		if value != nil {
			return value, row
		}
		row--
	}
	return nil, -1
}

func (b *bow) GetInt64(colIndex, rowIndex int) (int64, bool) {
	switch b.Schema().Field(colIndex).Type.ID() {
	case arrow.INT64:
		vd := array.NewInt64Data(b.Column(colIndex).Data())
		return vd.Value(rowIndex), vd.IsValid(rowIndex)
	case arrow.BOOL:
		vd := array.NewBooleanData(b.Column(colIndex).Data())
		booleanValue := vd.Value(rowIndex)
		if booleanValue {
			return 1, vd.IsValid(rowIndex)
		}
		return 0, vd.IsValid(rowIndex)
	default:
		panic(fmt.Sprintf("bow: unhandled type %s",
			b.Schema().Field(colIndex).Type.Name()))
	}
}

func (b *bow) GetPreviousInt64(col, row int) (int64, int) {
	if row < 0 || row >= b.NumRows() {
		return 0., -1
	}

	for row >= 0 {
		value, ok := b.GetInt64(col, row)
		if ok {
			return value, row
		}
		row--
	}
	return 0., -1
}

func (b *bow) GetFloat64(colIndex, rowIndex int) (float64, bool) {
	switch b.Schema().Field(colIndex).Type.ID() {
	case arrow.FLOAT64:
		vd := array.NewFloat64Data(b.Column(colIndex).Data())
		return vd.Value(rowIndex), vd.IsValid(rowIndex)
	case arrow.INT64:
		vd := array.NewInt64Data(b.Column(colIndex).Data())
		return float64(vd.Value(rowIndex)), vd.IsValid(rowIndex)
	case arrow.BOOL:
		vd := array.NewBooleanData(b.Column(colIndex).Data())
		booleanValue := vd.Value(rowIndex)
		if booleanValue {
			return 1., vd.IsValid(rowIndex)
		}
		return 0., vd.IsValid(rowIndex)
	case arrow.STRING:
		vd := array.NewStringData(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return ToFloat64(vd.Value(rowIndex))
		}
		return 0., false
	default:
		panic(fmt.Sprintf("bow: unhandled type %s",
			b.Schema().Field(colIndex).Type.Name()))
	}
}

func (b *bow) GetNextFloat64s(col1, col2, row int) (float64, float64, int) {
	if row < 0 || row >= b.NumRows() {
		return 0., 0., -1
	}

	for row >= 0 && row < b.NumRows() {
		var v1 float64
		v1, row = b.GetNextFloat64(col1, row)
		v2, row2 := b.GetNextFloat64(col2, row)
		if row == row2 {
			return v1, v2, row
		}

		row++
	}

	return 0., 0., -1
}

func (b *bow) GetNextFloat64(col, row int) (float64, int) {
	if row < 0 || row >= b.NumRows() {
		return 0., -1
	}

	for row < b.NumRows() {
		value, ok := b.GetFloat64(col, row)
		if ok {
			return value, row
		}
		row++
	}
	return 0., -1
}

func (b *bow) GetPreviousFloat64s(col1, col2, row int) (float64, float64, int) {
	if row < 0 || row >= b.NumRows() {
		return 0., 0., -1
	}

	for row >= 0 && row < b.NumRows() {
		var v1 float64
		v1, row = b.GetPreviousFloat64(col1, row)
		v2, row2 := b.GetPreviousFloat64(col2, row)
		if row == row2 {
			return v1, v2, row
		}

		row--
	}

	return 0., 0., -1
}

func (b *bow) GetPreviousFloat64(col, row int) (float64, int) {
	if row < 0 || row >= b.NumRows() {
		return 0., -1
	}

	for row >= 0 {
		value, ok := b.GetFloat64(col, row)
		if ok {
			return value, row
		}
		row--
	}
	return 0., -1
}

func (b *bow) GetType(colIndex int) Type {
	return getTypeFromArrowType(b.Schema().Field(colIndex).Type)
}

func (b *bow) GetName(colIndex int) (string, error) {
	if colIndex > len(b.Schema().Fields()) {
		return "", fmt.Errorf("no index %d", colIndex)
	}
	return b.Schema().Field(colIndex).Name, nil
}

func (b *bow) GetIndex(colName string) (int, error) {
	for i, field := range b.Schema().Fields() {
		if field.Name == colName {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no column '%s'", colName)
}

// Unused and similar to function GetIndex above
func (b *bow) GetColNameIndex(s string) int {
	for i, f := range b.Schema().Fields() {
		if f.Name == s {
			return i
		}
	}
	return -1
}
