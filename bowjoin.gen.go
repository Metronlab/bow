// Code generated by bowjoin.gen.go.tmpl. DO NOT EDIT.

package bow

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow/array"
)

func innerFillLeftBowCols(newSeries *[]Series, left *bow, newNumRows int,
	commonRows struct{ l, r []int }) {

	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		buf := NewBuffer(newNumRows, left.ColumnType(colIndex))
		switch left.ColumnType(colIndex) {
		case Int64:
			data := array.NewInt64Data(left.Column(colIndex).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.l[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.l[rowIndex]))
				}
			}
		case Float64:
			data := array.NewFloat64Data(left.Column(colIndex).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.l[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.l[rowIndex]))
				}
			}
		case Boolean:
			data := array.NewBooleanData(left.Column(colIndex).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.l[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.l[rowIndex]))
				}
			}
		case String:
			data := array.NewStringData(left.Column(colIndex).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.l[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.l[rowIndex]))
				}
			}
		default:
			panic(fmt.Errorf("unsupported type '%s'", left.ColumnType(colIndex)))
		}

		(*newSeries)[colIndex] = NewSeriesFromBuffer(left.ColumnName(colIndex), buf)
	}
}

func innerFillRightBowCols(newSeries *[]Series, left, right *bow, newNumRows, newNumCols int,
	commonCols map[string][]Buffer, commonRows struct{ l, r []int }) {
	var rightCol int

	for colIndex := left.NumCols(); colIndex < newNumCols; colIndex++ {
		buf := NewBuffer(newNumRows, right.ColumnType(rightCol))
		for commonCols[right.ColumnName(rightCol)] != nil {
			rightCol++
		}

		// Fill common rows from right bow
		switch right.ColumnType(rightCol) {
		case Int64:
			data := array.NewInt64Data(right.Column(rightCol).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.r[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.r[rowIndex]))
				}
			}
		case Float64:
			data := array.NewFloat64Data(right.Column(rightCol).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.r[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.r[rowIndex]))
				}
			}
		case Boolean:
			data := array.NewBooleanData(right.Column(rightCol).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.r[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.r[rowIndex]))
				}
			}
		case String:
			data := array.NewStringData(right.Column(rightCol).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.r[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.r[rowIndex]))
				}
			}
		default:
			panic(fmt.Errorf("unsupported type '%s'", right.ColumnType(rightCol)))
		}

		(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		rightCol++
	}
}

func outerFillLeftBowCols(newSeries *[]Series, left, right *bow, newNumRows, uniquesLeft int,
	commonCols map[string][]Buffer, commonRows struct{ l, r []int }) {
	var leftRow, commonRow int

	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		leftRow = 0
		commonRow = 0
		buf := NewBuffer(newNumRows, left.ColumnType(colIndex))

		// Fill rows from left bow
		switch left.ColumnType(colIndex) {
		case Int64:
			data := array.NewInt64Data(left.Column(colIndex).Data())
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if data.IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, data.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, data.Value(leftRow))
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
		case Float64:
			data := array.NewFloat64Data(left.Column(colIndex).Data())
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if data.IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, data.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, data.Value(leftRow))
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
		case Boolean:
			data := array.NewBooleanData(left.Column(colIndex).Data())
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if data.IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, data.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, data.Value(leftRow))
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
		case String:
			data := array.NewStringData(left.Column(colIndex).Data())
			for newRow := 0; left.NumRows() > 0 && newRow < newNumRows; newRow++ {
				if data.IsValid(leftRow) {
					buf.SetOrDropStrict(newRow, data.Value(leftRow))
				}
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(leftRow) {
						buf.SetOrDropStrict(newRow, data.Value(leftRow))
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
		default:
			panic(fmt.Errorf("unsupported type '%s'", left.ColumnType(colIndex)))
		}

		// Fill remaining rows from right bow if column is common
		_, isColCommon := commonCols[left.ColumnName(colIndex)]
		var newRow int
		if isColCommon {
			newRow = left.NumRows() + len(commonRows.l) - uniquesLeft
		}
		for rightRow := 0; isColCommon && rightRow < right.NumRows(); rightRow++ {
			var isRowCommon bool
			for i := 0; i < len(commonRows.r); i++ {
				if rightRow == commonRows.r[i] {
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

func outerFillRightBowCols(newSeries *[]Series, left, right *bow, newNumCols,
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
			data := array.NewInt64Data(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, data.Value(commonRows.r[commonRow]))
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
				for i := 0; i < len(commonRows.r); i++ {
					if rightRow == commonRows.r[i] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if data.IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, data.Value(rightRow))
					}
					newRow++
				}
			}
		case Float64:
			data := array.NewFloat64Data(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, data.Value(commonRows.r[commonRow]))
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
				for i := 0; i < len(commonRows.r); i++ {
					if rightRow == commonRows.r[i] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if data.IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, data.Value(rightRow))
					}
					newRow++
				}
			}
		case Boolean:
			data := array.NewBooleanData(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, data.Value(commonRows.r[commonRow]))
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
				for i := 0; i < len(commonRows.r); i++ {
					if rightRow == commonRows.r[i] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if data.IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, data.Value(rightRow))
					}
					newRow++
				}
			}
		case String:
			data := array.NewStringData(right.Column(rightCol).Data())

			// Fill common rows from right bow
			for newRow := 0; newRow < newNumRows; newRow++ {
				for commonRow < len(commonRows.l) &&
					leftRow == commonRows.l[commonRow] &&
					newRow < newNumRows {
					if data.IsValid(commonRows.r[commonRow]) {
						buf.SetOrDropStrict(newRow, data.Value(commonRows.r[commonRow]))
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
				for i := 0; i < len(commonRows.r); i++ {
					if rightRow == commonRows.r[i] {
						isRowCommon = true
						break
					}
				}
				if !isRowCommon {
					if data.IsValid(rightRow) {
						buf.SetOrDropStrict(newRow, data.Value(rightRow))
					}
					newRow++
				}
			}
		default:
			panic(fmt.Errorf("unsupported type '%s'", right.ColumnType(rightCol)))
		}
		(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		rightCol++
	}
}
