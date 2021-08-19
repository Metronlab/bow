package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var sortedTestBow, _ = NewBow(
	NewSeriesFromBuffer(Int64.String(), NewBufferFromData(
		[]int64{0, 1, 0, 0},
		[]bool{true, true, false, true})),
	NewSeriesFromBuffer(Float64.String(), NewBufferFromData(
		[]float64{0., 1., 0., 0.},
		[]bool{true, true, false, true})),
	NewSeriesFromBuffer(String.String(), NewBufferFromData(
		[]string{"0", "1", "0", "0"},
		[]bool{true, true, false, true})),
	NewSeriesFromBuffer(Boolean.String(), NewBufferFromData(
		[]bool{false, true, false, false},
		[]bool{true, true, false, true})),
)

func TestBow_Find(t *testing.T) {
	type toto int
	for i := 0; i < sortedTestBow.NumCols(); i++ {
		t.Run(sortedTestBow.ColumnName(i), func(t *testing.T) {
			v := sortedTestBow.GetValue(i, 0)
			assert.Equal(t, 0, sortedTestBow.Find(i, v))
			assert.Equal(t, 2, sortedTestBow.Find(i, nil))
			assert.Equal(t, -1, sortedTestBow.Find(i, toto(0)))
			assert.False(t, sortedTestBow.Contains(i, toto(0)))
			assert.True(t, sortedTestBow.Contains(i, v))
			assert.Equal(t, 3, sortedTestBow.FindNext(i, 1, v))

			empty := sortedTestBow.NewEmptySlice()
			assert.Equal(t, -1, empty.Find(i, v))
			assert.Equal(t, -1, empty.Find(i, nil))
			assert.Equal(t, -1, empty.Find(i, toto(0)))
			assert.False(t, empty.Contains(i, v))
			assert.Equal(t, -1, empty.FindNext(i, 1, v))
		})
	}
}
