package bow

import (
	"fmt"
	"sort"
	"sync"

	"github.com/apache/arrow/go/arrow/array"
)

// SortByCol returns a new Bow with the rows sorted by a column in ascending order.
// The only type currently supported for the column to sort by is Int64, without nil values.
// Returns the same Bow if the column is already sorted.
func (b *bow) SortByCol(colName string) (Bow, error) {
	if b.NumCols() == 0 {
		return nil, fmt.Errorf("bow.SortByCol: empty bow")
	}

	colToSortByIndex, err := b.ColumnIndex(colName)
	if err != nil {
		return nil, fmt.Errorf("bow.SortByCol: %w", err)
	}

	if b.Column(colToSortByIndex).NullN() != 0 {
		return nil, fmt.Errorf(
			"bow.SortByCol: column %s to sort by has %d nil values",
			colName, b.Column(colToSortByIndex).NullN())
	}

	if b.NumRows() == 0 {
		return b, nil
	}

	if b.ColumnType(colToSortByIndex) != Int64 {
		return nil, fmt.Errorf("bow.SortByCol: unsupported type for the column to sort by (Int64 only)")
	}

	// Build the Int64Slice interface to store the row indices before sorting
	var colToSortBy Int64Slice
	unsortedData := b.Column(colToSortByIndex).Data()
	colToSortBy.values = array.NewInt64Data(unsortedData).Int64Values()
	colToSortBy.indices = func() []int {
		res := make([]int, b.NumRows())
		for i := range res {
			res[i] = i
		}
		return res
	}()

	// Stop if sort by column is already sorted
	if IsInt64SliceSorted(colToSortBy) {
		return b, nil
	}

	// Sort the column by ascending values
	sort.Sort(colToSortBy)

	var seriesSlice = make([]Series, b.NumCols())
	seriesSlice[colToSortByIndex] = Series{
		Name:            b.ColumnName(colToSortByIndex),
		Data:            colToSortBy.values,
		nullBitmapBytes: b.Column(colToSortByIndex).NullBitmapBytes(),
	}

	// Reflect row order changes to fill the other columns
	var wg sync.WaitGroup
	for colIndex := range b.Schema().Fields() {
		if colIndex == colToSortByIndex {
			continue
		}

		wg.Add(1)
		go func(colIndex int, wg *sync.WaitGroup) {
			defer wg.Done()
			prevData := b.Column(colIndex).Data()
			switch b.ColumnType(colIndex) {
			case Int64:
				prevValues := array.NewInt64Data(prevData)
				seriesSlice[colIndex] = NewSeriesEmpty(b.ColumnName(colIndex), b.NumRows(), Int64)
				for i := 0; i < b.NumRows(); i++ {
					if prevValues.IsValid(colToSortBy.indices[i]) {
						seriesSlice[colIndex].SetOrDropStrict(i, prevValues.Value(colToSortBy.indices[i]))
					} else {
						seriesSlice[colIndex].SetOrDropStrict(i, nil)
					}
				}
			case Float64:
				prevValues := array.NewFloat64Data(prevData)
				seriesSlice[colIndex] = NewSeriesEmpty(b.ColumnName(colIndex), b.NumRows(), Float64)
				for i := 0; i < b.NumRows(); i++ {
					if prevValues.IsValid(colToSortBy.indices[i]) {
						seriesSlice[colIndex].SetOrDropStrict(i, prevValues.Value(colToSortBy.indices[i]))
					} else {
						seriesSlice[colIndex].SetOrDropStrict(i, nil)
					}
				}
			case Boolean:
				prevValues := array.NewBooleanData(prevData)
				seriesSlice[colIndex] = NewSeriesEmpty(b.ColumnName(colIndex), b.NumRows(), Boolean)
				for i := 0; i < b.NumRows(); i++ {
					if prevValues.IsValid(colToSortBy.indices[i]) {
						seriesSlice[colIndex].SetOrDropStrict(i, prevValues.Value(colToSortBy.indices[i]))
					} else {
						seriesSlice[colIndex].SetOrDropStrict(i, nil)
					}
				}
			case String:
				prevValues := array.NewStringData(prevData)
				seriesSlice[colIndex] = NewSeriesEmpty(b.ColumnName(colIndex), b.NumRows(), String)
				for i := 0; i < b.NumRows(); i++ {
					if prevValues.IsValid(colToSortBy.indices[i]) {
						seriesSlice[colIndex].SetOrDropStrict(i, prevValues.Value(colToSortBy.indices[i]))
					} else {
						seriesSlice[colIndex].SetOrDropStrict(i, nil)
					}
				}
			default:
				panic(fmt.Sprintf("bow: SortByCol function: unhandled type %s",
					b.Schema().Field(colIndex).Type.Name()))
			}
		}(colIndex, &wg)
	}
	wg.Wait()

	rec, err := newRecordFromSeries(b.Metadata(), seriesSlice...)
	if err != nil {
		return nil, fmt.Errorf("bow.SortByCol: %w", err)
	}

	return &bow{Record: rec}, nil
}

// IsInt64SliceSorted tests whether a column of int64s is sorted in increasing order.
func IsInt64SliceSorted(col Int64Slice) bool { return sort.IsSorted(col) }

// Int64Slice implements the methods of sort.Interface, sorting in increasing order
// (not-a-number values are treated as less than other values).
type Int64Slice struct {
	values  []int64
	indices []int
}

func (p Int64Slice) Len() int           { return len(p.indices) }
func (p Int64Slice) Less(i, j int) bool { return p.values[i] < p.values[j] }
func (p Int64Slice) Swap(i, j int) {
	p.values[i], p.values[j] = p.values[j], p.values[i]
	p.indices[i], p.indices[j] = p.indices[j], p.indices[i]
}
