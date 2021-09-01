package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerator(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		b, err := NewGenBow()
		assert.NoError(t, err)
		assert.Equal(t, genDefaultRows, b.NumRows())
		assert.Equal(t, genDefaultCols, b.NumCols())
		assert.Equal(t, Int64, b.ColumnType(0))

		b2, err := b.DropNils()
		assert.NoError(t, err)
		assert.Equal(t, b, b2)
		assert.True(t, b2.Equal(b), fmt.Sprintf("want %v\ngot %v", b, b2))
	})

	t.Run("with missing data", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenRows(1000000),
			OptionGenMissingData([]int{0, 1, 2}))
		assert.NoError(t, err)
		b2, err := b.DropNils()
		assert.NoError(t, err)
		assert.Less(t, b2.NumRows(), b.NumRows())
	})

	t.Run("float64 with all columns sorted", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenRows(8),
			OptionGenColTypes([]Type{Float64, Float64}))
		assert.NoError(t, err)

		assert.Equal(t, 8, b.NumRows())
		assert.Equal(t, 2, b.NumCols())
		assert.Equal(t, Float64, b.ColumnType(0))
		assert.Equal(t, Float64, b.ColumnType(1))
		assert.True(t, b.IsColSorted(0))
	})

	t.Run("descending sort on last column", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenCols(3),
			OptionGenStrategies([]GenStrategy{
				GenStrategyIncremental,
				GenStrategyIncremental,
				GenStrategyDecremental}),
		)
		assert.NoError(t, err)
		sorted := b.IsColSorted(genDefaultCols - 1)
		assert.True(t, sorted)
	})

	t.Run("custom names and types", func(t *testing.T) {
		b, err := NewGenBow(
			OptionGenCols(4),
			OptionGenColNames([]string{"A", "B", "C", "D"}),
			OptionGenColTypes([]Type{Int64, Float64, String, Boolean}),
		)
		assert.NoError(t, err)

		assert.Equal(t, "A", b.ColumnName(0))
		assert.Equal(t, "B", b.ColumnName(1))
		assert.Equal(t, "C", b.ColumnName(2))
		assert.Equal(t, "D", b.ColumnName(3))

		assert.Equal(t, Int64, b.ColumnType(0))
		assert.Equal(t, Float64, b.ColumnType(1))
		assert.Equal(t, String, b.ColumnType(2))
		assert.Equal(t, Boolean, b.ColumnType(3))
	})
}
