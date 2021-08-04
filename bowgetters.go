package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

func (b *bow) GetRow(rowIndex int) map[string]interface{} {
	row := map[string]interface{}{}
	for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
		val := b.GetValue(colIndex, rowIndex)
		if val == nil {
			continue
		}
		row[b.Schema().Field(colIndex).Name] = val
	}
	return row
}

func (b *bow) GetValueByName(colName string, rowIndex int) interface{} {
	for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
		if colName == b.Schema().Field(colIndex).Name {
			return b.GetValue(colIndex, rowIndex)
		}
	}
	return nil
}

func (b *bow) GetValue(colIndex, rowIndex int) interface{} {
	buf := b.NewBufferFromCol(colIndex)
	if !buf.Valid[rowIndex] {
		return nil
	}
	return buf.GetValue(rowIndex)
}

func (b *bow) GetNextValues(colIndex1, colIndex2, rowIndex int) (interface{}, interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 interface{}
		v1, rowIndex = b.GetNextValue(colIndex1, rowIndex)
		v2, row2 := b.GetNextValue(colIndex2, rowIndex)
		if rowIndex == row2 {
			return v1, v2, rowIndex
		}
		rowIndex++
	}
	return nil, nil, -1
}

func (b *bow) GetNextValue(colIndex, rowIndex int) (interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value := b.GetValue(colIndex, rowIndex)
		if value != nil {
			return value, rowIndex
		}
		rowIndex++
	}
	return nil, -1
}

func (b *bow) GetPreviousValues(colIndex1, colIndex2, rowIndex int) (interface{}, interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 interface{}
		v1, rowIndex = b.GetPreviousValue(colIndex1, rowIndex)
		v2, row2 := b.GetPreviousValue(colIndex2, rowIndex)
		if rowIndex == row2 {
			return v1, v2, rowIndex
		}
		rowIndex--
	}
	return nil, nil, -1
}

func (b *bow) GetPreviousValue(colIndex, rowIndex int) (interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value := b.GetValue(colIndex, rowIndex)
		if value != nil {
			return value, rowIndex
		}
		rowIndex--
	}
	return nil, -1
}

func (b *bow) GetInt64(colIndex, rowIndex int) (int64, bool) {
	if rowIndex < 0 || rowIndex >= b.NumRows() {
		return 0, false
	}

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
		panic(fmt.Sprintf("bow.GetInt64: unsupported type %s",
			b.Schema().Field(colIndex).Type.Name()))
	}
}

func (b *bow) GetNextInt64(colIndex, rowIndex int) (int64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetInt64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex++
	}
	return 0., -1
}

func (b *bow) GetPreviousInt64(colIndex, rowIndex int) (int64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetInt64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex--
	}
	return 0., -1
}

func (b *bow) GetFloat64(colIndex, rowIndex int) (float64, bool) {
	if rowIndex < 0 || rowIndex >= b.NumRows() {
		return 0., false
	}

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

func (b *bow) GetNextFloat64s(colIndex1, colIndex2, rowIndex int) (float64, float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 float64
		v1, rowIndex = b.GetNextFloat64(colIndex1, rowIndex)
		v2, row2 := b.GetNextFloat64(colIndex2, rowIndex)
		if rowIndex == row2 {
			return v1, v2, rowIndex
		}
		rowIndex++
	}
	return 0., 0., -1
}

func (b *bow) GetNextFloat64(colIndex, rowIndex int) (float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetFloat64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex++
	}
	return 0., -1
}

func (b *bow) GetPreviousFloat64s(colIndex1, colIndex2, rowIndex int) (float64, float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 float64
		v1, rowIndex = b.GetPreviousFloat64(colIndex1, rowIndex)
		v2, rowIndex2 := b.GetPreviousFloat64(colIndex2, rowIndex)
		if rowIndex == rowIndex2 {
			return v1, v2, rowIndex
		}
		rowIndex--
	}
	return 0., 0., -1
}

func (b *bow) GetPreviousFloat64(colIndex, rowIndex int) (float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetFloat64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex--
	}
	return 0., -1
}

func (b *bow) GetColType(colIndex int) Type {
	return getTypeFromArrowType(b.Schema().Field(colIndex).Type)
}

func (b *bow) GetColName(colIndex int) string {
	return b.Schema().Field(colIndex).Name
}

func (b *bow) GetColIndex(colName string) (int, error) {
	colIndices := b.Schema().FieldIndices(colName)
	if len(colIndices) == 0 {
		return -1, fmt.Errorf("no column '%s'", colName)
	}
	if len(colIndices) > 1 {
		return -1, fmt.Errorf("several columns '%s", colName)
	}
	return colIndices[0], nil
}

// FindFirst returns the row index of provided value's first occurrence in the dataset.
// Return -1 when value is not found.
func (b *bow) FindFirst(colIndex int, value interface{}) (rowIndex int) {
	var rowValue interface{}

	valueToFind := b.GetColType(colIndex).Convert(value)
	for row := 0; row < b.NumRows(); {
		rowValue, rowIndex = b.GetNextValue(colIndex, row)
		if rowIndex == -1 || rowValue == valueToFind {
			return
		}
		row = rowIndex + 1
	}
	return -1
}
