package bow

import (
	"fmt"
	"sort"
)

// SortByCol returns a new Bow with the rows sorted by a column in ascending order.
// Returns the same Bow if the column is already sorted.
func (b *bow) SortByCol(colIndex int) (Bow, error) {
	if b.Column(colIndex).NullN() != 0 {
		return nil, fmt.Errorf(
			"column to sort by has %d nil values",
			b.Column(colIndex).NullN())
	}

	sortableBuf := newBufferWithIndices(b.NewBufferFromCol(colIndex))
	// Stop if sort by column is already sorted
	if sortableBuf.IsSorted() {
		return b, nil
	}

	// Sort the column by ascending values
	sort.Sort(sortableBuf)

	// Fill the sort by column with sorted values
	sortedSeries := make([]Series, b.NumCols())
	for i := 0; i < b.NumCols(); i++ {
		if i == colIndex {
			sortedSeries[i] = NewSeriesFromBuffer(b.ColumnName(i), sortableBuf.Buffer)
			continue
		}
		buf := NewBuffer(b.NumRows(), b.ColumnType(i))
		for j, index := range sortableBuf.indices {
			buf.SetOrDropStrict(j, b.GetValue(i, index))
		}
		sortedSeries[i] = NewSeriesFromBuffer(b.ColumnName(i), buf)
	}

	return NewBowWithMetadata(b.Metadata(), sortedSeries...)
}

// bufferWithIndices implements the methods of sort.Interface, sorting in ascending order.
type bufferWithIndices struct {
	Buffer
	indices []int
}

func newBufferWithIndices(buf Buffer) bufferWithIndices {
	indices := make([]int, buf.Len())
	for i := 0; i < buf.Len(); i++ {
		indices[i] = i
	}
	return bufferWithIndices{Buffer: buf, indices: indices}
}

func (p bufferWithIndices) Swap(i, j int) {
	p.Buffer.Swap(i, j)
	p.indices[i], p.indices[j] = p.indices[j], p.indices[i]
}
