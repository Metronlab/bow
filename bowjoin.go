package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

func (b *bow) OuterJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}

	commonCols := getCommonCols(left.Schema(), right.Schema())

	// Compute new columns number
	newNumCols := left.NumCols() + right.NumCols() - len(commonCols)

	// Get common rows indices
	var commonRows struct{ l, r []int }
	for leftRow := 0; len(commonCols) > 0 && leftRow < left.NumRows(); leftRow++ {
		for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
			isRowCommon := true
			for _, colIndex := range commonCols {
				// TODO: improve performance by replacing GetValue by accessing array.Data values directly
				if left.GetValue(colIndex[0], leftRow) != right.GetValue(colIndex[1], rightRow) {
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
	newNumRows := left.NumRows() + right.NumRows()
	newNumRows += len(commonRows.l) - uniquesLeft - uniquesRight

	newSeries := make([]Series, newNumCols)

	fillLeftBowColumns(&newSeries, left, right, newNumRows,
		uniquesLeft, commonCols, commonRows)
	fillRightBowColumns(&newSeries, left, right, newNumCols,
		newNumRows, uniquesLeft, commonCols, commonRows)

	newBow, err := NewBow(nil, newSeries...)
	if err != nil {
		panic(err)
	}

	return newBow
}

func fillLeftBowColumns(newSeries *[]Series, left, right *bow, newNumRows, uniquesLeft int,
	commonCols map[string][]int, commonRows struct{ l, r []int }) {
	var leftRow, commonRow int
	newValids := make([]bool, newNumRows)

	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		leftRow = 0
		commonRow = 0
		for i := 0; i < newNumRows; i++ {
			newValids[i] = false
		}
		switch left.GetType(colIndex) {
		case Int64:
			leftData := array.NewInt64Data(left.Column(colIndex).Data())
			newArray := make([]int64, newNumRows)

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					newArray[newRow] = leftData.Value(leftRow)
					newValids[newRow] = true
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValids[newRow] = true
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
			var rightData *array.Int64
			var newRow int
			if isColCommon {
				rightData = array.NewInt64Data(right.Column(commonCols[left.ColumnName(colIndex)][1]).Data())
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
					if right.Column(commonCols[left.ColumnName(colIndex)][1]).IsValid(rightRow) {
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeries(
				left.ColumnName(colIndex),
				left.GetType(colIndex),
				newArray, newValids)
		case Float64:
			leftData := array.NewFloat64Data(left.Column(colIndex).Data())
			newArray := make([]float64, newNumRows)

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					newArray[newRow] = leftData.Value(leftRow)
					newValids[newRow] = true
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValids[newRow] = true
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
			var rightData *array.Float64
			var newRow int
			if isColCommon {
				rightData = array.NewFloat64Data(right.Column(commonCols[left.ColumnName(colIndex)][1]).Data())
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
					if right.Column(commonCols[left.ColumnName(colIndex)][1]).IsValid(rightRow) {
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeries(
				left.ColumnName(colIndex),
				left.GetType(colIndex),
				newArray, newValids)
		case Bool:
			leftData := array.NewBooleanData(left.Column(colIndex).Data())
			newArray := make([]bool, newNumRows)

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					newArray[newRow] = leftData.Value(leftRow)
					newValids[newRow] = true
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValids[newRow] = true
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
			var rightData *array.Boolean
			var newRow int
			if isColCommon {
				rightData = array.NewBooleanData(right.Column(commonCols[left.ColumnName(colIndex)][1]).Data())
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
					if right.Column(commonCols[left.ColumnName(colIndex)][1]).IsValid(rightRow) {
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeries(
				left.ColumnName(colIndex),
				left.GetType(colIndex),
				newArray, newValids)
		case String:
			leftData := array.NewStringData(left.Column(colIndex).Data())
			newArray := make([]string, newNumRows)

			// Fill rows from left bow
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if left.Column(colIndex).IsValid(leftRow) {
					newArray[newRow] = leftData.Value(leftRow)
					newValids[newRow] = true
				}
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValids[newRow] = true
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
			var rightData *array.String
			var newRow int
			if isColCommon {
				rightData = array.NewStringData(right.Column(commonCols[left.ColumnName(colIndex)][1]).Data())
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
					if right.Column(commonCols[left.ColumnName(colIndex)][1]).IsValid(rightRow) {
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}

			(*newSeries)[colIndex] = NewSeries(
				left.ColumnName(colIndex),
				left.GetType(colIndex),
				newArray, newValids)
		}
	}
}

func fillRightBowColumns(newSeries *[]Series, left, right *bow, newNumCols,
	newNumRows, uniquesLeft int, commonCols map[string][]int,
	commonRows struct{ l, r []int }) {
	var leftRow, commonRow, rightCol int
	newValids := make([]bool, newNumRows)

	for colIndex := left.NumCols(); colIndex < newNumCols; colIndex++ {
		leftRow = 0
		commonRow = 0
		for i := 0; i < newNumRows; i++ {
			newValids[i] = false
		}
		for commonCols[right.ColumnName(rightCol)] != nil {
			rightCol++
		}
		switch right.GetType(rightCol) {
		case Int64:
			rightData := array.NewInt64Data(right.Column(rightCol).Data())
			newArray := make([]int64, newNumRows)

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow && newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValids[newRow] = true
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
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeries(
				right.ColumnName(rightCol),
				right.GetType(rightCol),
				newArray, newValids)
		case Float64:
			rightData := array.NewFloat64Data(right.Column(rightCol).Data())
			newArray := make([]float64, newNumRows)

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValids[newRow] = true
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
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeries(
				right.ColumnName(rightCol),
				right.GetType(rightCol),
				newArray, newValids)
		case Bool:
			rightData := array.NewBooleanData(right.Column(rightCol).Data())
			newArray := make([]bool, newNumRows)

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValids[newRow] = true
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
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeries(
				right.ColumnName(rightCol),
				right.GetType(rightCol),
				newArray, newValids)
		case String:
			rightData := array.NewStringData(right.Column(rightCol).Data())
			newArray := make([]string, newNumRows)

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					commonRows.l[commonRow] == leftRow &&
					newRow < newNumRows {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValids[newRow] = true
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
						newArray[newRow] = rightData.Value(rightRow)
						newValids[newRow] = true
					}
					newRow++
				}
			}
			(*newSeries)[colIndex] = NewSeries(
				right.ColumnName(rightCol),
				right.GetType(rightCol),
				newArray, newValids)
		}
		rightCol++
	}
}

func (b *bow) InnerJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}

	if left.NumRows() <= 0 && right.NumRows() <= 0 {
		return &bow{}
	}
	if left.NumRows() > 0 && right.NumRows() <= 0 {
		return left
	}
	if left.NumRows() <= 0 && right.NumRows() > 0 {
		return right
	}

	commonCols := getCommonCols(left.Schema(), right.Schema())

	// Compute new columns number
	newNumCols := left.NumCols() + right.NumCols() - len(commonCols)

	// Get common rows indexes
	var commonRows struct{ l, r []int }
	for leftRow := 0; len(commonCols) > 0 && leftRow < left.NumRows(); leftRow++ {
		for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
			isCommon := true
			for _, commonCol := range commonCols {
				// TODO: improve performance by replacing GetValue by accessing array.Data values directly
				if left.GetValue(commonCol[0], leftRow) != right.GetValue(commonCol[1], rightRow) {
					isCommon = false
					continue
				}
			}
			if isCommon {
				commonRows.l = append(commonRows.l, leftRow)
				commonRows.r = append(commonRows.r, rightRow)
			}
		}
	}
	newNumRows := len(commonRows.l)

	var rightCol, leftRow, commonRow, newRow int
	newValid := make([]bool, newNumRows)
	newSeries := make([]Series, newNumCols)

	// Fill left bow columns
	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		commonRow = 0
		newRow = 0
		for i := 0; i < newNumRows; i++ {
			newValid[i] = false
		}
		switch left.GetType(colIndex) {
		case Int64:
			leftData := array.NewInt64Data(left.Column(colIndex).Data())
			newArray := make([]int64, newNumRows)
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeries(left.ColumnName(colIndex), left.GetType(colIndex), newArray, newValid)
		case Float64:
			leftData := array.NewFloat64Data(left.Column(colIndex).Data())
			newArray := make([]float64, newNumRows)
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeries(left.ColumnName(colIndex), left.GetType(colIndex), newArray, newValid)
		case Bool:
			leftData := array.NewBooleanData(left.Column(colIndex).Data())
			newArray := make([]bool, newNumRows)
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeries(left.ColumnName(colIndex), left.GetType(colIndex), newArray, newValid)
		case String:
			leftData := array.NewStringData(left.Column(colIndex).Data())
			newArray := make([]string, newNumRows)
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if left.Column(colIndex).IsValid(leftRow) {
						newArray[newRow] = leftData.Value(leftRow)
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[colIndex] = NewSeries(left.ColumnName(colIndex), left.GetType(colIndex), newArray, newValid)
		}
	}

	// Fill right bow columns
	for col := left.NumCols(); col < newNumCols; col++ {
		commonRow = 0
		newRow = 0
		for i := 0; i < newNumRows; i++ {
			newValid[i] = false
		}
		for commonCols[right.ColumnName(rightCol)] != nil {
			rightCol++
		}
		switch right.GetType(rightCol) {
		case Int64:
			rightData := array.NewInt64Data(right.Column(rightCol).Data())
			newArray := make([]int64, newNumRows)
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
		case Float64:
			rightData := array.NewFloat64Data(right.Column(rightCol).Data())
			newArray := make([]float64, newNumRows)
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
		case Bool:
			rightData := array.NewBooleanData(right.Column(rightCol).Data())
			newArray := make([]bool, newNumRows)
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
		case String:
			rightData := array.NewStringData(right.Column(rightCol).Data())
			newArray := make([]string, newNumRows)
			// Fill common rows from right bow
			for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
				if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
					if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
						newArray[newRow] = rightData.Value(commonRows.r[commonRow])
						newValid[newRow] = true
					}
					newRow++
					commonRow++
				}
			}
			newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
		}
		rightCol++
	}

	newBow, err := NewBow(nil, newSeries...)
	if err != nil {
		panic(err.Error())
	}

	return newBow
}

// getCommonCols returns in key column names and corresponding indexes on l / r
// TODO: improve behavior of multiple column with same name
func getCommonCols(l, r *arrow.Schema) map[string][]int {
	commonCols := make(map[string][]int)
	for _, lField := range l.Fields() {
		rFields, commonCol := r.FieldsByName(lField.Name)
		if commonCol {
			if len(rFields) > 1 {
				panic(fmt.Errorf(
					"bow Join: too many columns have the same name: right:%+v left:%+v",
					r.String(), l.String()))
			}
			rField := rFields[0]
			if rField.Type.ID() != lField.Type.ID() {
				panic(fmt.Errorf(
					"bow Join: left and right bow on join columns are of incompatible types: %s",
					lField.Name))
			}

			commonCols[lField.Name] = append(commonCols[lField.Name], l.FieldIndices(lField.Name)[0])
			commonCols[lField.Name] = append(commonCols[lField.Name], r.FieldIndices(lField.Name)[0])
		}
	}
	return commonCols
}
