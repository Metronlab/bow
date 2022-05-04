package bow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v8/arrow/array"
)

// InnerJoin joins columns of two Bows on common columns and rows.
// The Metadata of the two Bows are also joined by appending keys and values.
func (b *bow) InnerJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("non bow object passed as argument")
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

	// Get common columns indices
	commonCols := getCommonCols(left, right)

	// Get common rows indices
	commonRows := getCommonRows(left, right, commonCols)

	// Prepare new Series Slice
	newNumCols := left.NumCols() + right.NumCols() - len(commonCols)
	newSeries := make([]Series, newNumCols)
	newNumRows := len(commonRows.l)

	innerFillLeftBowCols(&newSeries, left,
		newNumRows, commonRows)
	innerFillRightBowCols(&newSeries, left, right,
		newNumRows, newNumCols, commonCols, commonRows)

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
		panic(err)
	}

	return newBow
}

// OuterJoin joins columns of two Bows on common columns, and keeps all rows.
// The Metadata of the two Bows are also joined by appending keys and values.
func (b *bow) OuterJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("non bow object passed as argument")
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

	outerFillLeftBowCols(&newSeries, left, right, newNumRows,
		uniquesLeft, commonCols, commonRows)
	outerFillRightBowCols(&newSeries, left, right, newNumCols,
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
		panic(err)
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
				"too many columns have the same name: right:%+v left:%+v",
				right.String(), left.String()))
		}

		rField := rFields[0]
		if rField.Type.ID() != lField.Type.ID() {
			panic(fmt.Errorf(
				"left and right bow on join columns are of incompatible types: %s",
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

func innerFillLeftBowCols(newSeries *[]Series, left *bow, newNumRows int,
	commonRows struct{ l, r []int }) {

	for colIndex := 0; colIndex < left.NumCols(); colIndex++ {
		buf := NewBuffer(newNumRows, left.ColumnType(colIndex))
		switch buf.DataType {
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
		case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
			data := array.NewTimestampData(left.Column(colIndex).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.l[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.l[rowIndex]))
				}
			}
		default:
			panic(fmt.Errorf("unsupported type '%s'", buf.DataType))
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
		switch buf.DataType {
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
		case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
			data := array.NewTimestampData(right.Column(rightCol).Data())
			for rowIndex := 0; rowIndex < newNumRows; rowIndex++ {
				if data.IsValid(commonRows.r[rowIndex]) {
					buf.SetOrDropStrict(rowIndex, data.Value(commonRows.r[rowIndex]))
				}
			}
		default:
			panic(fmt.Errorf("unsupported type '%s'", buf.DataType))
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
		switch buf.DataType {
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
		case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
			data := array.NewTimestampData(left.Column(colIndex).Data())
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
			panic(fmt.Errorf("unsupported type '%s'", buf.DataType))
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

		switch buf.DataType {
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
		case TimestampSec, TimestampMilli, TimestampMicro, TimestampNano:
			data := array.NewTimestampData(right.Column(rightCol).Data())

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
			panic(fmt.Errorf("unsupported type '%s'", buf.DataType))
		}

		(*newSeries)[colIndex] = NewSeriesFromBuffer(right.ColumnName(rightCol), buf)
		rightCol++
	}
}
