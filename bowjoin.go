package bow

import (
	"fmt"
	"sort"
)

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

	// Get common columns indices
	commonCols := getCommonCols(left, right)

	// Get common rows indices
	commonRows := getCommonRows(left, right, commonCols)

	// Prepare new Series Slice
	newNumCols := left.NumCols() + right.NumCols() - len(commonCols)
	newSeries := make([]Series, newNumCols)
	newNumRows := len(commonRows.l)

	innerFillLeftBowCols(&newSeries, left, right,
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
		panic(fmt.Errorf("bow.InnerJoin: %w", err))
	}

	return newBow
}

func (b *bow) OuterJoin(other Bow) Bow {
	left := b
	right, ok := other.(*bow)
	if !ok {
		panic("bow.OuterJoin: non bow object passed as argument")
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
		panic(fmt.Errorf("bow.OuterJoin: %w", err))
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

	fmt.Printf("commonRows l: %+v\n", commonRows.l)
	fmt.Printf("commonRows r: %+v\n", commonRows.r)

	return commonRows
}
