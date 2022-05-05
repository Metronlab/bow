package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
)

// GetRow returns the row `rowIndex`. Map keys represent column names.
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

// GetRowsChan returns a chan of all the rows. Map keys represent column names.
func (b *bow) GetRowsChan() <-chan map[string]interface{} {
	rows := make(chan map[string]interface{})
	go b.getRowsChan(rows)

	return rows
}

func (b *bow) getRowsChan(rows chan map[string]interface{}) {
	defer close(rows)

	if b.Record == nil || b.NumRows() == 0 {
		return
	}

	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		rows <- b.GetRow(rowIndex)
	}
}

// GetValue returns the value at row `rowIndex` and column `colIndex'.
func (b *bow) GetValue(colIndex, rowIndex int) interface{} {
	if b.Column(colIndex).IsNull(rowIndex) {
		return nil
	}

	switch b.ColumnType(colIndex) {
	case Float64:
		return array.NewFloat64Data(b.Column(colIndex).Data()).Value(rowIndex)
	case Int64:
		return array.NewInt64Data(b.Column(colIndex).Data()).Value(rowIndex)
	case Boolean:
		return array.NewBooleanData(b.Column(colIndex).Data()).Value(rowIndex)
	case String:
		return array.NewStringData(b.Column(colIndex).Data()).Value(rowIndex)
	default:
		panic(fmt.Errorf("unsupported type '%s'", b.ColumnType(colIndex)))
	}
}

// GetPrevValue returns the value and the index of the previous non-nil value in the column `colIndex`, starting from the row `rowIndex`.
// Returns nil and -1 if no value has been found.
func (b *bow) GetPrevValue(colIndex, rowIndex int) (interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value := b.GetValue(colIndex, rowIndex)
		if value != nil {
			return value, rowIndex
		}
		rowIndex--
	}

	return nil, -1
}

// GetNextValue returns the value and the index of the next non-nil value in the column `colIndex`, starting from the row `rowIndex`.
// Returns nil and -1 if no value has been found.
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

// GetPrevValues returns the two values and the index of the previous non-nil values in columns `colIndex1` and `colIndex2`, starting from row `rowIndex`.
// Returns nil, nil and -1 if no value has been found.
func (b *bow) GetPrevValues(colIndex1, colIndex2, rowIndex int) (interface{}, interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 interface{}
		v1, rowIndex = b.GetPrevValue(colIndex1, rowIndex)
		v2, rowIndex2 := b.GetPrevValue(colIndex2, rowIndex)
		if rowIndex == rowIndex2 {
			return v1, v2, rowIndex
		}
		rowIndex--
	}

	return nil, nil, -1
}

// GetNextValues returns the two values and the index of the next non-nil values in columns `colIndex1` and `colIndex2`, starting from row `rowIndex`.
// Returns nil, nil and -1 if no value has been found.
func (b *bow) GetNextValues(colIndex1, colIndex2, rowIndex int) (interface{}, interface{}, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 interface{}
		v1, rowIndex = b.GetNextValue(colIndex1, rowIndex)
		v2, rowIndex2 := b.GetNextValue(colIndex2, rowIndex)
		if rowIndex == rowIndex2 {
			return v1, v2, rowIndex
		}
		rowIndex++
	}

	return nil, nil, -1
}

// GetPrevRowIndex returns the index of the previous non-nil value in the column `colIndex`, starting from row `rowIndex`.
// Returns -1 if no value has been found.
func (b *bow) GetPrevRowIndex(colIndex, rowIndex int) int {
	col := b.Column(colIndex)
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		if col.IsValid(rowIndex) {
			return rowIndex
		}
		rowIndex--
	}

	return -1
}

// GetNextRowIndex returns the index of the next non-nil value in the column `colIndex`, starting from row `rowIndex`.
// Returns -1 if no value has been found.
func (b *bow) GetNextRowIndex(colIndex, rowIndex int) int {
	col := b.Column(colIndex)
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		if col.IsValid(rowIndex) {
			return rowIndex
		}
		rowIndex++
	}

	return -1
}

// GetInt64 returns the value as int64 from the column `colIndex` and row `rowindex`, and a bool whether the value is nil or not.
// Attempts to convert the value if the type of the column is not arrow.INT64.
func (b *bow) GetInt64(colIndex, rowIndex int) (int64, bool) {
	if rowIndex < 0 || rowIndex >= b.NumRows() {
		return 0, false
	}

	switch b.Schema().Field(colIndex).Type.ID() {
	case arrow.INT64:
		vd := array.NewInt64Data(b.Column(colIndex).Data())
		return vd.Value(rowIndex), vd.IsValid(rowIndex)
	case arrow.FLOAT64:
		vd := array.NewFloat64Data(b.Column(colIndex).Data())
		return int64(vd.Value(rowIndex)), vd.IsValid(rowIndex)
	case arrow.BOOL:
		vd := array.NewBooleanData(b.Column(colIndex).Data())
		booleanValue := vd.Value(rowIndex)
		if booleanValue {
			return 1, vd.IsValid(rowIndex)
		}
		return 0, vd.IsValid(rowIndex)
	case arrow.STRING:
		vd := array.NewStringData(b.Column(colIndex).Data())
		if vd.IsValid(rowIndex) {
			return ToInt64(vd.Value(rowIndex))
		}
		return 0., false
	default:
		panic(fmt.Errorf("unsupported type '%s'",
			b.Schema().Field(colIndex).Type.Name()))
	}
}

// GetPrevInt64 returns the previous non-nil value as int64 and its row index from the column `colIndex`, starting from row `rowindex`.
// Attempts to convert the value if the type of the column is not arrow.INT64.
// Returns 0 and -1 in case no value is found.
func (b *bow) GetPrevInt64(colIndex, rowIndex int) (int64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetInt64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex--
	}

	return 0, -1
}

// GetNextInt64 returns the next non-nil value as int64 and its row index from the column `colIndex`, starting from row `rowindex`.
// Attempts to convert the value if the type of the column is not arrow.INT64.
// Returns 0 and -1 in case no value is found.
func (b *bow) GetNextInt64(colIndex, rowIndex int) (int64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetInt64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex++
	}

	return 0, -1
}

// GetFloat64 returns the value as float64 from the column `colIndex` and row `rowindex`, and a bool whether the value is nil or not.
// Attempts to convert the value if the type of the column is not arrow.FLOAT64.
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
		panic(fmt.Sprintf("unsupported type '%s'",
			b.Schema().Field(colIndex).Type.Name()))
	}
}

// GetPrevFloat64 returns the previous non-nil value as float64 and its row index from the column `colIndex`, starting from row `rowindex`.
// Attempts to convert the value if the type of the column is not arrow.FLOAT64.
// Returns 0. and -1 in case no value is found.
func (b *bow) GetPrevFloat64(colIndex, rowIndex int) (float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		value, ok := b.GetFloat64(colIndex, rowIndex)
		if ok {
			return value, rowIndex
		}
		rowIndex--
	}

	return 0., -1
}

// GetNextFloat64 returns the next non-nil value as float64 and its row index from the column `colIndex`, starting from row `rowindex`.
// Attempts to convert the value if the type of the column is not arrow.FLOAT64.
// Returns 0. and -1 in case no value is found.
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

// GetPrevFloat64s returns the previous non-nil two values as float64 and their row index from columns `colIndex1` and `colIndex2`, starting from row `rowindex`.
// Attempts to convert the values if the type of the columns are not arrow.FLOAT64.
// Returns 0., 0. and -1 in case no index is found with two non-nil values.
func (b *bow) GetPrevFloat64s(colIndex1, colIndex2, rowIndex int) (float64, float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 float64
		v1, rowIndex = b.GetPrevFloat64(colIndex1, rowIndex)
		v2, rowIndex2 := b.GetPrevFloat64(colIndex2, rowIndex)
		if rowIndex == rowIndex2 {
			return v1, v2, rowIndex
		}
		rowIndex--
	}

	return 0., 0., -1
}

// GetNextFloat64s returns the next non-nil two values as float64 and their row index from columns `colIndex1` and `colIndex2`, starting from row `rowindex`.
// Attempts to convert the values if the type of the columns are not arrow.FLOAT64.
// Returns 0., 0. and -1 in case no index is found with two non-nil values.
func (b *bow) GetNextFloat64s(colIndex1, colIndex2, rowIndex int) (float64, float64, int) {
	for rowIndex >= 0 && rowIndex < b.NumRows() {
		var v1 float64
		v1, rowIndex = b.GetNextFloat64(colIndex1, rowIndex)
		v2, rowIndex2 := b.GetNextFloat64(colIndex2, rowIndex)
		if rowIndex == rowIndex2 {
			return v1, v2, rowIndex
		}
		rowIndex++
	}

	return 0., 0., -1
}

// ColumnType returns the Bow type from the column `colIndex`.
func (b *bow) ColumnType(colIndex int) Type {
	return getBowTypeFromArrowType(b.Schema().Field(colIndex).Type)
}

// ColumnIndex returns the index of the column with the name `colName`, and an error.
// Return an error if the column doesn't exist or if several columns have this name.
func (b *bow) ColumnIndex(colName string) (int, error) {
	colIndices := b.Schema().FieldIndices(colName)
	if len(colIndices) == 0 {
		return -1, fmt.Errorf("no column '%s'", colName)
	}

	if len(colIndices) > 1 {
		return -1, fmt.Errorf("several columns '%s'", colName)
	}

	return colIndices[0], nil
}

// Distinct returns all non-nil different values found in the column `colIndex` in a new Bow.
func (b *bow) Distinct(colIndex int) Bow {
	hitMap := make(map[interface{}]struct{})
	for i := 0; i < b.NumRows(); i++ {
		val := b.GetValue(colIndex, i)
		if val != nil {
			hitMap[val] = struct{}{}
		}
	}

	buf := NewBuffer(len(hitMap), b.ColumnType(colIndex))
	i := 0
	for k := range hitMap {
		buf.SetOrDropStrict(i, k)
		i++
	}

	sort.Sort(buf)

	res, err := NewBow(NewSeriesFromBuffer(b.ColumnName(colIndex), buf))
	if err != nil {
		panic(err)
	}

	return res
}
