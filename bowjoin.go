package bow

import (
	"errors"
	"github.com/apache/arrow/go/arrow/array"
	"log"
	"sort"
)

func (b *bow) OuterJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}

	if (left.Record == nil || left.NumRows() <= 0) && (right.Record == nil || right.NumRows() <= 0) {
		return &bow{}
	}
	if left.Record != nil && left.NumRows() > 0 && (right.Record == nil || right.NumRows() <= 0) {
		return left
	}
	if right.Record != nil && right.NumRows() > 0 && (left.Record == nil || left.NumRows() <= 0) {
		return right
	}

	// Get common columns names and indexes
	commonCols := make(map[string][]int)
	for _, lField := range left.Schema().Fields() {
		rField, commonCol := right.Schema().FieldByName(lField.Name)
		if commonCol {
			if rField.Type.ID() != lField.Type.ID() {
				log.Panicf("bow: OuterJoin: common columns are of incompatible data types: " + lField.Name)
			}
			commonCols[lField.Name] = append(commonCols[lField.Name], left.Schema().FieldIndex(lField.Name))
			commonCols[lField.Name] = append(commonCols[lField.Name], right.Schema().FieldIndex(rField.Name))
		}
	}
	// fmt.Printf("commonCols:%+v\n", commonCols)

	// Compute new columns number
	newColNum := left.NumCols() + right.NumCols() - len(commonCols)

	// Get common rows indexes
	var commonRows [2][]int
	for leftRow := 0; leftRow < left.NumRows(); leftRow++ {
		for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
			isCommon := true
			// fmt.Printf("- leftRow:%d rightRow:%d\n", leftRow, rightRow)
			for _, colIndex := range commonCols {
				// fmt.Printf("%s left:%+v right:%+v\n", col, left.GetValue(colIndex[0], leftRow), right.GetValue(colIndex[1], rightRow))
				if left.GetValue(colIndex[0], leftRow) != right.GetValue(colIndex[1], rightRow) {
					isCommon = false
					continue
				}
			}
			if isCommon {
				commonRows[0] = append(commonRows[0], leftRow)
				commonRows[1] = append(commonRows[1], rightRow)
			}
		}
	}
	// fmt.Printf("\ncommonRows:%+v\n\n", commonRows)

	// Compute new rows number
	var uniquesLeft, uniquesRight = 1, 1
	sortedLeft := make([]int, len(commonRows[0]))
	sortedRight := make([]int, len(commonRows[1]))
	copy(sortedLeft, commonRows[0])
	copy(sortedRight, commonRows[1])
	sort.Ints(sortedLeft)
	sort.Ints(sortedRight)
	for i := 0; i < len(commonRows[0])-1; i++ {
		if sortedLeft[i] != sortedLeft[i+1] {
			uniquesLeft++
		}
		if sortedRight[i] != sortedRight[i+1] {
			uniquesRight++
		}
	}
	newRowNum := left.NumRows() + right.NumRows()
	newRowNum += len(commonRows[0]) - uniquesLeft - uniquesRight

	// Fill newSeries
	var rightCol int
	newSeries := make([]Series, newColNum)
	for col := 0; col < newColNum; col++ {
		// Fill left bow columns
		if col < left.NumCols() {
			switch left.GetType(col) {
			case Int64:
				leftValues := array.NewInt64Data(left.Column(col).Data()).Int64Values()
				newArray := make([]int64, newRowNum)
				newValid := make([]bool, newRowNum)
				var leftRow int
				var commonRow int
				for newRow := 0; newRow < newRowNum; newRow++ {
					if left.Column(col).IsValid(leftRow) {
						newArray[newRow] = leftValues[leftRow]
						newValid[newRow] = true
					}
					for commonRow < len(commonRows[0]) && commonRows[0][commonRow] == leftRow && newRow < newRowNum {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftValues[leftRow]
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows[0]) && commonRows[0][commonRow+1] == leftRow {
							newRow++
						}
						commonRow++
					}
					leftRow++
				}

				// Fill remaining rows from right bow if column is common
				if _, isColCommon := commonCols[left.ColumnName(col)]; isColCommon {
					newRow := left.NumRows() + len(commonRows[0]) - uniquesLeft
					rightValues := array.NewInt64Data(right.Column(commonCols[left.ColumnName(col)][1]).Data()).Int64Values()
					for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
						var isRowCommon bool
						for commonRow := 0; commonRow < len(commonRows[1]); commonRow++ {
							if rightRow == commonRows[1][commonRow] {
								isRowCommon = true
								break
							}
						}
						if !isRowCommon {
							if right.Column(commonCols[left.ColumnName(col)][1]).IsValid(rightRow) {
								newArray[newRow] = rightValues[rightRow]
								newValid[newRow] = true
							}
							newRow++
						}
					}
				}
				newSeries[col] = NewSeries(left.ColumnName(col), left.GetType(col), newArray, newValid)
			case Float64:
				leftValues := array.NewFloat64Data(left.Column(col).Data()).Float64Values()
				newArray := make([]float64, newRowNum)
				newValid := make([]bool, newRowNum)
				var leftRow int
				var commonRow int
				for newRow := 0; newRow < newRowNum; newRow++ {
					if left.Column(col).IsValid(leftRow) {
						newArray[newRow] = leftValues[leftRow]
						newValid[newRow] = true
					}
					for commonRow < len(commonRows[0]) && commonRows[0][commonRow] == leftRow && newRow < newRowNum {
						if left.Column(col).IsValid(leftRow) {
							newArray[newRow] = leftValues[leftRow]
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows[0]) && commonRows[0][commonRow+1] == leftRow {
							newRow++
						}
						commonRow++
					}
					leftRow++
				}

				// Fill remaining rows from right bow if column is common
				if _, isColCommon := commonCols[left.ColumnName(col)]; isColCommon {
					newRow := left.NumRows() + len(commonRows[0]) - uniquesLeft
					rightValues := array.NewFloat64Data(right.Column(commonCols[left.ColumnName(col)][1]).Data()).Float64Values()
					for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
						var isRowCommon bool
						for commonRow := 0; commonRow < len(commonRows[1]); commonRow++ {
							if rightRow == commonRows[1][commonRow] {
								isRowCommon = true
								break
							}
						}
						if !isRowCommon {
							if right.Column(commonCols[left.ColumnName(col)][1]).IsValid(rightRow) {
								newArray[newRow] = rightValues[rightRow]
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
				rightValues := array.NewInt64Data(right.Column(rightCol).Data()).Int64Values()
				newArray := make([]int64, newRowNum)
				newValid := make([]bool, newRowNum)
				var leftRow int
				var commonRow int
				// Fill common rows from right bow
				for newRow := 0; newRow < newRowNum; newRow++ {
					for commonRow < len(commonRows[0]) && commonRows[0][commonRow] == leftRow && newRow < newRowNum {
						if right.Column(rightCol).IsValid(commonRows[1][commonRow]) {
							newArray[newRow] = rightValues[commonRows[1][commonRow]]
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows[0]) && commonRows[0][commonRow+1] == leftRow {
							newRow++
						}
						commonRow++
					}
					leftRow++
				}
				// Fill remaining rows from right bow
				newRow := left.NumRows() + len(commonRows[1]) - uniquesLeft
				for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
					var isRowCommon bool
					for commonRow := 0; commonRow < len(commonRows[1]); commonRow++ {
						if rightRow == commonRows[1][commonRow] {
							isRowCommon = true
							break
						}
					}
					if !isRowCommon {
						if right.Column(rightCol).IsValid(rightRow) {
							newArray[newRow] = rightValues[rightRow]
							newValid[newRow] = true
						}
						newRow++
					}
				}
				newSeries[col] = NewSeries(right.ColumnName(rightCol), right.GetType(rightCol), newArray, newValid)
			case Float64:
				rightValues := array.NewFloat64Data(right.Column(rightCol).Data()).Float64Values()
				newArray := make([]float64, newRowNum)
				newValid := make([]bool, newRowNum)
				var leftRow int
				var commonRow int
				// Fill common rows from right bow
				for newRow := 0; newRow < newRowNum; newRow++ {
					for commonRow < len(commonRows[0]) && commonRows[0][commonRow] == leftRow && newRow < newRowNum {
						if right.Column(rightCol).IsValid(commonRows[1][commonRow]) {
							newArray[newRow] = rightValues[commonRows[1][commonRow]]
							newValid[newRow] = true
						}
						if commonRow+1 < len(commonRows[0]) && commonRows[0][commonRow+1] == leftRow {
							newRow++
						}
						commonRow++
					}
					leftRow++
				}
				// Fill remaining rows from right bow
				newRow := left.NumRows() + len(commonRows[1]) - uniquesLeft
				for rightRow := 0; rightRow < right.NumRows(); rightRow++ {
					var isRowCommon bool
					for commonRow := 0; commonRow < len(commonRows[1]); commonRow++ {
						if rightRow == commonRows[1][commonRow] {
							isRowCommon = true
							break
						}
					}
					if !isRowCommon {
						if right.Column(rightCol).IsValid(rightRow) {
							newArray[newRow] = rightValues[rightRow]
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

// TODO: used series directly
// For each resulting row, every values is filled first with all left bow columns then right uncommon columns
// If several values are present on right on same indexes, the left indexes/values will be duplicated
// left bow:         right bow:
// index col         index col2
// 1     1           1     1
//                   1     2
// result:
// index col col2
// 1     1   1
// 1     1   2
func (b *bow) InnerJoin(other Bow) Bow {
	b2, ok := other.(*bow)
	if !ok {
		panic("bow: non bow object passed as argument")
	}
	commonCols := map[string]struct{}{}
	for _, lField := range b.Schema().Fields() {
		rField, found := b2.Schema().FieldByName(lField.Name)
		if found {
			if rField.Type.ID() != lField.Type.ID() {
				panic(errors.New("bow: left and right bow on join columns are of incompatible types: " + lField.Name))
			}
			commonCols[lField.Name] = struct{}{}
		}
	}
	var rColIndexes []int
	for i, rField := range b2.Schema().Fields() {
		if _, ok := commonCols[rField.Name]; !ok {
			rColIndexes = append(rColIndexes, i)
		}
	}
	for name := range commonCols {
		b2.newIndex(name)
	}
	resultInterfaces := make([][]interface{}, len(b.Schema().Fields())+len(rColIndexes))
	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		for _, rValIndex := range b.getRightBowIndexesAtRow(b2, commonCols, rowIndex) {
			for colIndex := range b.Schema().Fields() {
				resultInterfaces[colIndex] = append(resultInterfaces[colIndex], b.GetValue(colIndex, rowIndex))
			}
			for i, rColIndex := range rColIndexes {
				resultInterfaces[len(b.Schema().Fields())+i] =
					append(resultInterfaces[len(b.Schema().Fields())+i], b2.GetValue(rColIndex, rValIndex))
			}
		}
	}
	colNames := make([]string, len(b.Schema().Fields())+len(rColIndexes))
	colTypes := make([]Type, len(b.Schema().Fields())+len(rColIndexes))
	for i, f := range b.Schema().Fields() {
		colNames[i] = f.Name
		colTypes[i] = b.GetType(i)
	}
	for i, index := range rColIndexes {
		colNames[len(b.Schema().Fields())+i] = b2.Schema().Field(index).Name
		colTypes[len(b.Schema().Fields())+i] = b2.GetType(index)
	}
	res, err := NewBowFromColumnBasedInterfaces(colNames, colTypes, resultInterfaces)
	if err != nil {
		panic(err)
	}
	return res
}

func (b *bow) getRightBowIndexesAtRow(b2 *bow, commonColumns map[string]struct{}, rowIndex int) []int {
	var possibleIndexes [][]int
	for name := range commonColumns {
		val := b.GetValue(b.Schema().FieldIndex(name), rowIndex)
		if val == nil {
			return []int{}
		}
		index, ok := b2.indexes[name]
		if !ok {
			return []int{}
		}
		indexes, ok := index.m[val]
		if !ok {
			return []int{}
		}
		possibleIndexes = append(possibleIndexes, indexes)
	}
	if len(possibleIndexes) == 0 {
		return []int{}
	}
	res := possibleIndexes[0]
	if len(possibleIndexes) == 1 {
		return res
	}
	for _, ints := range possibleIndexes[1:] {
		start := res
		res = []int{}
		for _, i := range ints {
			for _, j := range start {
				if i == j {
					res = append(res, i)
				}
			}
		}
	}
	return res
}

type index struct {
	t Type
	m map[interface{}][]int
}

func (b *bow) newIndex(colName string) {
	if _, found := b.Schema().FieldByName(colName); !found {
		panic("bow: try to build index on non existing columns")
	}
	// return if index already exists
	if _, found := b.indexes[colName]; found {
		return
	}
	colIndex := b.Schema().FieldIndex(colName)
	dType := b.GetType(colIndex)
	m := make(map[interface{}][]int)
	for i := 0; i < b.NumRows(); i++ {
		val := b.GetValue(colIndex, i)
		if val != nil {
			if _, found := m[val]; !found {
				m[val] = []int{i}
			} else {
				m[val] = append(m[val], i)
			}
		}
	}
	if b.indexes == nil {
		b.indexes = map[string]index{}
	}
	b.indexes[colName] = index{t: dType, m: m}
}
