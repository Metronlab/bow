package bow

import (
	"errors"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"sort"
)

func (b *bow) OuterJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}

	commonCols := getCommonCols(left.Schema(), right.Schema())

	// Compute new columns number
	newColNum := left.NumCols() + right.NumCols() - len(commonCols)

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
	newRowNum := left.NumRows() + right.NumRows()
	newRowNum += len(commonRows.l) - uniquesLeft - uniquesRight

	// Fill newSeries
	var rightCol int
	newSeries := make([]Series, newColNum)
	newValid := make([]bool, newRowNum)
	var leftRow int
	var commonRow int
	for col := 0; col < newColNum; col++ {
		leftRow = 0
		commonRow = 0
		for i := 0; i < newRowNum; i++ {
			newValid[i] = false
		}
		// Fill left bow columns
		if col < left.NumCols() {
			switch left.GetType(col) {
			case Int64:
				leftData := array.NewInt64Data(left.Column(col).Data())
				newArray := make([]int64, newRowNum)

				// Fill rows from left bow
				if left.NumRows() > 0 {
					for newRow := 0; newRow < newRowNum; newRow++ {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
							if left.Column(col).IsValid(leftRow) {
								newArray[newRow] = leftData.Value(leftRow)
								newValid[newRow] = true
							}
							if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
								newRow++
							}
							commonRow++
						}
						if leftRow++; leftRow >= left.NumRows() {
							break
						}
					}
				}

				// Fill remaining rows from right bow if column is common
				if _, isColCommon := commonCols[left.ColumnName(col)]; isColCommon {
					rightData := array.NewInt64Data(right.Column(commonCols[left.ColumnName(col)][1]).Data())
					newRow := left.NumRows() + len(commonRows.l) - uniquesLeft
					for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
						var isRowCommon bool
						for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
							if rightRow == commonRows.r[commonRow] {
								isRowCommon = true
								break
							}
						}
						if !isRowCommon {
							if right.Column(commonCols[left.ColumnName(col)][1]).IsValid(rightRow) {
								newArray[newRow] = rightData.Value(rightRow)
								newValid[newRow] = true
							}
							newRow++
						}
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case Float64:
				leftData := array.NewFloat64Data(left.Column(col).Data())
				newArray := make([]float64, newRowNum)

				// Fill rows from left bow
				if left.NumRows() > 0 {
					for newRow := 0; newRow < newRowNum; newRow++ {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
							if left.Column(col).IsValid(leftRow) {
								newArray[newRow] = leftData.Value(leftRow)
								newValid[newRow] = true
							}
							if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
								newRow++
							}
							commonRow++
						}
						if leftRow++; leftRow >= left.NumRows() {
							break
						}
					}
				}

				// Fill remaining rows from right bow if column is common
				if _, isColCommon := commonCols[left.ColumnName(col)]; isColCommon {
					rightData := array.NewFloat64Data(right.Column(commonCols[left.ColumnName(col)][1]).Data())
					newRow := left.NumRows() + len(commonRows.l) - uniquesLeft
					for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
						var isRowCommon bool
						for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
							if rightRow == commonRows.r[commonRow] {
								isRowCommon = true
								break
							}
						}
						if !isRowCommon {
							if right.Column(commonCols[left.ColumnName(col)][1]).IsValid(rightRow) {
								newArray[newRow] = rightData.Value(rightRow)
								newValid[newRow] = true
							}
							newRow++
						}
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case Bool:
				leftData := array.NewBooleanData(left.Column(col).Data())
				newArray := make([]bool, newRowNum)

				// Fill rows from left bow
				if left.NumRows() > 0 {
					for newRow := 0; newRow < newRowNum; newRow++ {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
							if left.Column(col).IsValid(leftRow) {
								newArray[newRow] = leftData.Value(leftRow)
								newValid[newRow] = true
							}
							if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
								newRow++
							}
							commonRow++
						}
						if leftRow++; leftRow >= left.NumRows() {
							break
						}
					}
				}

				// Fill remaining rows from right bow if column is common
				if _, isColCommon := commonCols[left.ColumnName(col)]; isColCommon {
					rightData := array.NewBooleanData(right.Column(commonCols[left.ColumnName(col)][1]).Data())
					newRow := left.NumRows() + len(commonRows.l) - uniquesLeft
					for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
						var isRowCommon bool
						for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
							if rightRow == commonRows.r[commonRow] {
								isRowCommon = true
								break
							}
						}
						if !isRowCommon {
							if right.Column(commonCols[left.ColumnName(col)][1]).IsValid(rightRow) {
								newArray[newRow] = rightData.Value(rightRow)
								newValid[newRow] = true
							}
							newRow++
						}
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case String:
				leftData := array.NewStringData(left.Column(col).Data())
				newArray := make([]string, newRowNum)

				// Fill rows from left bow
				if left.NumRows() > 0 {
					for newRow := 0; newRow < newRowNum; newRow++ {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
							if left.Column(col).IsValid(leftRow) {
								newArray[newRow] = leftData.Value(leftRow)
								newValid[newRow] = true
							}
							if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
								newRow++
							}
							commonRow++
						}
						if leftRow++; leftRow >= left.NumRows() {
							break
						}
					}
				}

				// Fill remaining rows from right bow if column is common
				if _, isColCommon := commonCols[left.ColumnName(col)]; isColCommon {
					rightData := array.NewStringData(right.Column(commonCols[left.ColumnName(col)][1]).Data())
					newRow := left.NumRows() + len(commonRows.l) - uniquesLeft
					for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
						var isRowCommon bool
						for commonRow := 0; commonRow < len(commonRows.r); commonRow++ {
							if rightRow == commonRows.r[commonRow] {
								isRowCommon = true
								break
							}
						}
						if !isRowCommon {
							if right.Column(commonCols[left.ColumnName(col)][1]).IsValid(rightRow) {
								newArray[newRow] = rightData.Value(rightRow)
								newValid[newRow] = true
							}
							newRow++
						}
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			}
			// Fill right bow columns
		} else {
			for commonCols[right.ColumnName(rightCol)] != nil {
				rightCol++
			}
			switch right.GetType(rightCol) {
			case Int64:
				rightData := array.NewInt64Data(right.Column(rightCol).Data())
				newArray := make([]int64, newRowNum)

				// Fill common rows from right bow
				for newRow := 0; newRow < newRowNum; newRow++ {
					for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
						if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
							newArray[newRow] = rightData.Value(commonRows.r[commonRow])
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
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
							newValid[newRow] = true
						}
						newRow++
					}
				}
				newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
			case Float64:
				rightData := array.NewFloat64Data(right.Column(rightCol).Data())
				newArray := make([]float64, newRowNum)

				// Fill common rows from right bow
				for newRow := 0; newRow < newRowNum; newRow++ {
					for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
						if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
							newArray[newRow] = rightData.Value(commonRows.r[commonRow])
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
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
							newValid[newRow] = true
						}
						newRow++
					}
				}
				newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
			case Bool:
				rightData := array.NewBooleanData(right.Column(rightCol).Data())
				newArray := make([]bool, newRowNum)

				// Fill common rows from right bow
				for newRow := 0; newRow < newRowNum; newRow++ {
					for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
						if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
							newArray[newRow] = rightData.Value(commonRows.r[commonRow])
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
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
							newValid[newRow] = true
						}
						newRow++
					}
				}
				newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
			case String:
				rightData := array.NewStringData(right.Column(rightCol).Data())
				newArray := make([]string, newRowNum)

				// Fill common rows from right bow
				for newRow := 0; newRow < newRowNum; newRow++ {
					for commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow && newRow < newRowNum {
						if right.Column(rightCol).IsValid(commonRows.r[commonRow]) {
							newArray[newRow] = rightData.Value(commonRows.r[commonRow])
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows.l) && commonRows.l[commonRow+1] == leftRow {
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
							newValid[newRow] = true
						}
						newRow++
					}
				}
				newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
			}
			rightCol++
		}
	}
	newBow, err := NewBow(newSeries...)
	if err != nil {
		panic(err.Error())
	}
	return newBow
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
	newColNum := left.NumCols() + right.NumCols() - len(commonCols)

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
	newRowNum := len(commonRows.l)

	// Fill newSeries
	var rightCol, leftRow, commonRow, newRow int
	newValid := make([]bool, newRowNum)
	newSeries := make([]Series, newColNum)
	for col := 0; col < newColNum; col++ {
		commonRow = 0
		newRow = 0
		for i := 0; i < newRowNum; i++ {
			newValid[i] = false
		}
		// Fill left bow columns
		if col < left.NumCols() {
			switch left.GetType(col) {
			case Int64:
				leftData := array.NewInt64Data(left.Column(col).Data())
				newArray := make([]int64, newRowNum)
				for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
					if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						newRow++
						commonRow++
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case Float64:
				leftData := array.NewFloat64Data(left.Column(col).Data())
				newArray := make([]float64, newRowNum)
				for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
					if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						newRow++
						commonRow++
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case Bool:
				leftData := array.NewBooleanData(left.Column(col).Data())
				newArray := make([]bool, newRowNum)
				for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
					if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						newRow++
						commonRow++
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case String:
				leftData := array.NewStringData(left.Column(col).Data())
				newArray := make([]string, newRowNum)
				for leftRow = 0; leftRow < left.NumRows(); leftRow++ {
					if commonRow < len(commonRows.l) && commonRows.l[commonRow] == leftRow {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftData.Value(leftRow)
							newValid[newRow] = true
						}
						newRow++
						commonRow++
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			}
			// Fill right bow columns
		} else {
			for commonCols[right.ColumnName(rightCol)] != nil {
				rightCol++
			}
			switch right.GetType(rightCol) {
			case Int64:
				rightData := array.NewInt64Data(right.Column(rightCol).Data())
				newArray := make([]int64, newRowNum)
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
				newArray := make([]float64, newRowNum)
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
				newArray := make([]bool, newRowNum)
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
				newArray := make([]string, newRowNum)
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
	}
	newBow, err := NewBow(newSeries...)
	if err != nil {
		panic(err.Error())
	}
	return newBow
}

// getCommonCols return in key column names and corresponding indexes on l / r
// TODO: improve behavior of multiple column with same name
func getCommonCols(l, r *arrow.Schema) map[string][]int {
	commonCols := make(map[string][]int)
	for _, lField := range l.Fields() {
		rFields, commonCol := r.FieldsByName(lField.Name)
		if commonCol {
			if len(rFields) > 1 {
				panic("too many columns carry the same name")
			}
			rField := rFields[0]
			if rField.Type.ID() != lField.Type.ID() {
				panic(errors.New("bow: left and right bow on join columns are of incompatible types: " + lField.Name))
			}

			commonCols[lField.Name] = append(commonCols[lField.Name], l.FieldIndices(lField.Name)[0])
			commonCols[lField.Name] = append(commonCols[lField.Name], r.FieldIndices(lField.Name)[0])
		}
	}
	return commonCols
}
