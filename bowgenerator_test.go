package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerator(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		b, err := NewGenBow()
		assert.Nil(t, err)
		assert.Equal(t, 10, b.NumRows())
		assert.Equal(t, 10, b.NumCols())
		assert.Equal(t, Int64, b.ColumnType(0))

		b2, err := b.DropNils()
		assert.Nil(t, err)
		assert.Equal(t, b, b2)
		assert.True(t, b2.Equal(b), fmt.Sprintf("want %v\ngot %v", b, b2))
	})

	t.Run("stdDataset", func(t *testing.T) {
		assert.Equal(t, "", stdDataSet.String())
	})
	t.Run("with missing data", func(t *testing.T) {
		b, err := NewGenBow(OptionGenMissingData(true))
		assert.Nil(t, err)
		b2, err := b.DropNils()
		assert.Nil(t, err)
		assert.Less(t, b2.NumRows(), b.NumRows())
	})

	t.Run("float64 with first column sorted", func(t *testing.T) {
		b, err := NewGenBow(OptionGenRows(8), OptionGenCols(2), OptionGenDataType(Float64), OptionGenRefCol(0, false))
		assert.Nil(t, err)

		assert.Equal(t, 8, b.NumRows())
		assert.Equal(t, 2, b.NumCols())
		assert.Equal(t, Float64, b.ColumnType(0))
		assert.Equal(t, Float64, b.ColumnType(1))
		sorted := b.IsColSorted(0)
		assert.True(t, sorted)
	})

	t.Run("descending sort on last column", func(t *testing.T) {
		b, err := NewGenBow(OptionGenRefCol(9, true))
		assert.Nil(t, err)
		sorted := b.IsColSorted(9)
		assert.True(t, sorted)
	})

	t.Run("custom names and types", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenCols(4),
			OptionGenColNames([]string{"A", "B", "C", "D"}),
			OptionGenDataTypes([]Type{Int64, Float64, String, Boolean}),
			OptionGenRefCol(0, true),
		)
		assert.Nil(t, err)

		sorted := b.IsColSorted(0)
		assert.True(t, sorted)

		n := b.ColumnName(0)
		assert.Equal(t, "A", n)
		n = b.ColumnName(1)
		assert.Equal(t, "B", n)
		n = b.ColumnName(2)
		assert.Equal(t, "C", n)
		n = b.ColumnName(3)
		assert.Equal(t, "D", n)

		assert.Equal(t, Int64, b.ColumnType(0))
		assert.Equal(t, Float64, b.ColumnType(1))
		assert.Equal(t, String, b.ColumnType(2))
		assert.Equal(t, Boolean, b.ColumnType(3))
	})
}
