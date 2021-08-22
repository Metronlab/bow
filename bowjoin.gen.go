// Code generated by bowjoin.gen.go.tmpl. DO NOT EDIT.

package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/arrow/array"
)

func (b *bow) OuterJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}

	// Get common columns indices
	commonCols := getCommonCols(left, right)

	// Get common rows indices
	commonRows := getCommonRows(left, right, commonCols)

	// Compute new rows number
	var uniquesLeft, uniquesRight int
	if len(commonRows.l) > 0 {
		uniquesLeft, uniquesRight = 1, 1
		sortedLeft := make([]int, len(commonRows.l))
		sortedRight := make([]int, len(commonRows.l))
		copy(sortedLeft, commonRows.l)
		copy(sortedRight, commonRows.r)
		sort.Ints(sortedLeft)
		sort.Ints(sortedRight)
		for i := 0; i < len(commonRows.l)-1; i++ {
			if sortedLeft[i] != sortedLeft[i+1] {
				uniquesLeft++
			}
			if sortedRight[i] != sortedRight[i+1] {
				uniquesRight++
			}
		}
	}
	newNumRows := left.NumRows() + right.NumRows() +
		len(commonRows.l) - uniquesLeft - uniquesRight

	// Prepare new Series Slice
	newNumCols := left.NumCols() + right.NumCols() - len(commonCols)
	newSeries := make([]Series, newNumCols)

	fillLeftBowColumns(&newSeries, left, right, newNumRows,
		uniquesLeft, commonCols, commonRows)
	fillRightBowColumns(&newSeries, left, right, newNumCols,
		newNumRows, uniquesLeft, commonCols, commonRows)

	// Join Metadata
	var keys, values []string
	keys = append(keys, left.Schema().Metadata().Keys()...)
	keys = append(keys, right.Schema().Metadata().Keys()...)
	values = append(values, left.Schema().Metadata().Values()...)
	values = append(values, right.Schema().Metadata().Values()...)

	newBow, err := NewBowWithMetadata(
		NewMetadata(keys, values),
		newSeries...)
	if err != nil {
		panic(fmt.Errorf("bow.OuterJoin: %w", err))
	}

	return newBow
}

func fillLeftBowColumns(newSeries *[]Series, left, right *bow, newNumRows, uniquesLeft int,
	commonCols map[string][]Buffer, commonRows struct{ l, r []int }) {
	var leftRow, commonRow int

	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		leftRow = 0
		commonRow = 0
		buf := NewBuffer(newNumRows, left.ColumnType(colIndex))
		switch left.ColumnType(colIndex) {
		case Int64:
			leftData := array.NewInt64Data(left.Column(colIndex).Data())

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				if leftRow++; leftRow >= left.NumRows() {
					break
				}
			}

			// Fill remaining rows from right bow if column is common
			_, isColCommon := commonCols[left.ColumnName(colIndex)]
			var newRow int
			if isColCommon {
				newRow = left.NumRows() + len(commonRows.l) - uniquesLeft
			}
			for rightRow := 0; isColCommon && rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					buf.SetOrDropStrict(newRow, commonCols[left.ColumnName(colIndex)][1].GetValue(rightRow))
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		case Float64:
			leftData := array.NewFloat64Data(left.Column(colIndex).Data())

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				if leftRow++; leftRow >= left.NumRows() {
					break
				}
			}

			// Fill remaining rows from right bow if column is common
			_, isColCommon := commonCols[left.ColumnName(colIndex)]
			var newRow int
			if isColCommon {
				newRow = left.NumRows() + len(commonRows.l) - uniquesLeft
			}
			for rightRow := 0; isColCommon && rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					buf.SetOrDropStrict(newRow, commonCols[left.ColumnName(colIndex)][1].GetValue(rightRow))
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		case Boolean:
			leftData := array.NewBooleanData(left.Column(colIndex).Data())

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				if leftRow++; leftRow >= left.NumRows() {
					break
				}
			}

			// Fill remaining rows from right bow if column is common
			_, isColCommon := commonCols[left.ColumnName(colIndex)]
			var newRow int
			if isColCommon {
				newRow = left.NumRows() + len(commonRows.l) - uniquesLeft
			}
			for rightRow := 0; isColCommon && rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					buf.SetOrDropStrict(newRow, commonCols[left.ColumnName(colIndex)][1].GetValue(rightRow))
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		case String:
			leftData := array.NewStringData(left.Column(colIndex).Data())

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				if leftRow++; leftRow >= left.NumRows() {
					break
				}
			}

			// Fill remaining rows from right bow if column is common
			_, isColCommon := commonCols[left.ColumnName(colIndex)]
			var newRow int
			if isColCommon {
				newRow = left.NumRows() + len(commonRows.l) - uniquesLeft
			}
			for rightRow := 0; isColCommon && rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					buf.SetOrDropStrict(newRow, commonCols[left.ColumnName(colIndex)][1].GetValue(rightRow))
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		}
	}
}

func fillRightBowColumns(newSeries *[]Series, left, right *bow, newNumCols,
	newNumRows, uniquesLeft int, commonCols map[string][]Buffer,
	commonRows struct{ l, r []int }) {
	var leftRow, commonRow, rightCol int

	for colIndex := left.NumCols(); colIndex < newNumCols; colIndex++ {
		leftRow = 0
		commonRow = 0
		for commonCols[right.ColumnName(rightCol)] != nil {
			rightCol++
		}
		buf := NewBuffer(newNumRows, right.ColumnType(rightCol))
		switch right.ColumnType(rightCol) {
		case Int64:
			rightData := array.NewInt64Data(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow && newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				leftRow++
			}

			// Fill remaining rows from right bow
			newRow := left.NumRows() + len(commonRows.r) - uniquesLeft
			for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if right.Column(rightCol).IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, rightData.Value(rightRow))
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		case Float64:
			rightData := array.NewFloat64Data(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow && newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				leftRow++
			}

			// Fill remaining rows from right bow
			newRow := left.NumRows() + len(commonRows.r) - uniquesLeft
			for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if right.Column(rightCol).IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, rightData.Value(rightRow))
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		case Boolean:
			rightData := array.NewBooleanData(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow && newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				leftRow++
			}

			// Fill remaining rows from right bow
			newRow := left.NumRows() + len(commonRows.r) - uniquesLeft
			for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if right.Column(rightCol).IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, rightData.Value(rightRow))
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		case String:
			rightData := array.NewStringData(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow && newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					if commonRow+1 < len(commonRows.l) &&
						commonRows.l[commonRow+1] == leftRow {
						newRow++
					}
					commonRow++
				}
				leftRow++
			}

			// Fill remaining rows from right bow
			newRow := left.NumRows() + len(commonRows.r) - uniquesLeft
			for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
				var isRowCommon bool
				for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
					if rightRow == commonRows.r[commonRow] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if right.Column(rightCol).IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, rightData.Value(rightRow))
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		}
		rightCol++
	}
}

func (b *bow) InnerJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow.InnerJoin: non bow object passed as argument")
	}

	if left.NumCols() == 0 && right.NumCols() == 0 {
		return left.NewSlice(0, 0)
	}

	if left.NumCols() > 0 && right.NumCols() == 0 {
		return left.NewSlice(0, 0)
	}

	if left.NumCols() == 0 && right.NumCols() > 0 {
		return right.NewSlice(0, 0)
	}

	// Get common columns indexes
	commonCols := getCommonCols(left, right)

	// Get common rows indexes
	commonRows := getCommonRows(left, right, commonCols)

	// Prepare new Series Slice
	newNumCols := left.NumCols() + right.NumCols() - len(commonCols)
	newSeries := make([]Series, newNumCols)

	newNumRows := len(commonRows.l)
	var rightCol, leftRow, commonRow, newRow int

	// Fill left bow columns
	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		newRow = 0
		commonRow = 0
		buf := NewBuffer(newNumRows, left.ColumnType(colIndex))
		switch left.ColumnType(colIndex) {
		case Int64:
			leftData := array.NewInt64Data(left.Column(colIndex).Data())
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		case Float64:
			leftData := array.NewFloat64Data(left.Column(colIndex).Data())
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		case Boolean:
			leftData := array.NewBooleanData(left.Column(colIndex).Data())
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		case String:
			leftData := array.NewStringData(left.Column(colIndex).Data())
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, leftData.Value(leftRow))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
		}
	}

	// Fill right bow columns
	for colIndex := left.NumCols(); colIndex < newNumCols; colIndex++ {
		newRow = 0
		commonRow = 0
		buf := NewBuffer(newNumRows, right.ColumnType(rightCol))
		for commonCols[right.ColumnName(rightCol)] != nil {
			rightCol++
		}
		switch right.ColumnType(rightCol) {
		case Int64:
			rightData := array.NewInt64Data(right.Column(rightCol).Data())
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		case Float64:
			rightData := array.NewFloat64Data(right.Column(rightCol).Data())
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		case Boolean:
			rightData := array.NewBooleanData(right.Column(rightCol).Data())
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		case String:
			rightData := array.NewStringData(right.Column(rightCol).Data())
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, rightData.Value(commonRows.r[commonRow]))
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		}
		rightCol++
	}

	// Join Metadata
	var keys, values []string
	keys = append(keys, left.Schema().Metadata().Keys()...)
	keys = append(keys, right.Schema().Metadata().Keys()...)
	values = append(values, left.Schema().Metadata().Values()...)
	values = append(values, right.Schema().Metadata().Values()...)

	newBow, err := NewBowWithMetadata(
		NewMetadata(keys, values),
		newSeries...)
	if err != nil {
		panic(fmt.Errorf("bow.InnerJoin: %w", err))
	}

	return newBow
}

// getCommonCols returns in key column names and corresponding buffers on left / right schemas
func getCommonCols(left, right Bow) map[string][]Buffer {
	commonCols := make(map[string][]Buffer)
	for _, lField := range left.Schema().Fields() {
		rFields, commonCol := right.Schema().FieldsByName(lField.Name)
		if !commonCol {
			continue
		}

		if len(rFields) > 1 {
			panic(fmt.Errorf(
				"bow.Join: too many columns have the same name: right:%+v left:%+v",
				right.String(), left.String()))
		}

		rField := rFields[0]
		if rField.Type.ID() != lField.Type.ID() {
			panic(fmt.Errorf(
				"bow.Join: left and right bow on join columns are of incompatible types: %s",
				lField.Name))
		}

		commonCols[lField.Name] = []Buffer{
			left.NewBufferFromCol(left.Schema().FieldIndices(lField.Name)[0]),
			right.NewBufferFromCol(right.Schema().FieldIndices(lField.Name)[0])}
	}

	return commonCols
}

type CommonRows struct {
	l, r []int
}

func getCommonRows(left, right Bow, commonColBufs map[string][]Buffer) CommonRows {
	var commonRows CommonRows

	if len(commonColBufs) == 0 {
		return commonRows
	}

	for leftRow := 0; leftRow < left.NumRows(); leftRow++ {
		for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
			isRowCommon := true
			for _, colBufs := range commonColBufs {
				if colBufs[0].GetValue(leftRow) != colBufs[1].GetValue(rightRow) {
					isRowCommon = false
					continue
				}
			}

			if isRowCommon {
				commonRows.l = append(commonRows.l, leftRow)
				commonRows.r = append(commonRows.r, rightRow)
			}
		}
	}

	return commonRows
}