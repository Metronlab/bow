package bow

import (
	"fmt"
	"sort"
	"sync"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
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

	var colToSortBy Int64Slice
	var newArray array.Interface
	prevData := b.Record.Column(colToSortByIndex).Data()
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch b.ColumnType(colToSortByIndex) {
	case Int64:
		// Build the Int64Slice interface to store the row indices before sorting
		colToSortBy.indices = func() []int {
			res := make([]int, b.NumRows())
			for i := range res {
				res[i] = i
			}
			return res
		}()
		prevValues := array.NewInt64Data(prevData)
		colToSortBy.values = prevValues.Int64Values()
		colToSortBy.valid = getValid(prevValues, b.NumRows())

		// Stop if sort by column is already sorted
		if IsInt64SliceSorted(colToSortBy) {
			return b, nil
		}

		// Sort the column by ascending values
		sort.Sort(colToSortBy)

		builder := array.NewInt64Builder(pool)
		builder.AppendValues(colToSortBy.values, colToSortBy.valid)
		newArray = builder.NewArray()
	default:
		return nil, fmt.Errorf("bow.SortByCol: unsupported type for the column to sort by (Int64 only)")
	}

	// Interpolate the sort by column with sorted values
	sortedSeries := make([]Series, b.NumCols())
	sortedSeries[colToSortByIndex] = Series{
		Name:  colName,
		Array: newArray,
	}

	// Reflect row order changes to fill the other columns
	var wg sync.WaitGroup
	for colIndex, col := range b.Schema().Fields() {
		if col.Name == colName {
			continue
		}

		wg.Add(1)
		go func(colIndex int, col arrow.Field, wg *sync.WaitGroup) {
			defer wg.Done()
			var newArray array.Interface
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			newValids := make([]bool, b.NumRows())
			prevData := b.Record.Column(colIndex).Data()
			switch b.ColumnType(colIndex) {
			case Int64:
				prevValues := array.NewInt64Data(prevData)
				newValues := make([]int64, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.indices[i])
					if prevValues.IsValid(colToSortBy.indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewInt64Builder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			case Float64:
				prevValues := array.NewFloat64Data(prevData)
				newValues := make([]float64, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.indices[i])
					if prevValues.IsValid(colToSortBy.indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewFloat64Builder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			case Bool:
				prevValues := array.NewBooleanData(prevData)
				newValues := make([]bool, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.indices[i])
					if prevValues.IsValid(colToSortBy.indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewBooleanBuilder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			case String:
				prevValues := array.NewStringData(prevData)
				newValues := make([]string, b.NumRows())
				for i := 0; i < b.NumRows(); i++ {
					newValues[i] = prevValues.Value(colToSortBy.indices[i])
					if prevValues.IsValid(colToSortBy.indices[i]) {
						newValids[i] = true
					}
				}
				builder := array.NewStringBuilder(pool)
				builder.AppendValues(newValues, newValids)
				newArray = builder.NewArray()
			default:
				panic(fmt.Sprintf("bow: SortByCol function: unhandled type %s",
					b.Schema().Field(colIndex).Type.Name()))
			}
			sortedSeries[colIndex] = Series{
				Name:  col.Name,
				Array: newArray,
			}
		}(colIndex, col, &wg)
	}
	wg.Wait()

	return NewBowWithMetadata(b.Metadata(), sortedSeries...)
}

// IsInt64SliceSorted tests whether a column of int64s is sorted in increasing order.
func IsInt64SliceSorted(col Int64Slice) bool { return sort.IsSorted(col) }

// Int64Slice implements the methods of sort.Interface, sorting in increasing order
// (not-a-number values are treated as less than other values).
type Int64Slice struct {
	values  []int64
	valid   []bool
	indices []int
}

func (p Int64Slice) Len() int           { return len(p.indices) }
func (p Int64Slice) Less(i, j int) bool { return p.values[i] < p.values[j] }
func (p Int64Slice) Swap(i, j int) {
	p.values[i], p.values[j] = p.values[j], p.values[i]
	p.valid[i], p.valid[j] = p.valid[j], p.valid[i]
	p.indices[i], p.indices[j] = p.indices[j], p.indices[i]
}
