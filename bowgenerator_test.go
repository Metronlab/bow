package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerator(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		bow, err := NewGenBow()
		assert.Nil(t, err)
		assert.Equal(t, 10, bow.NumRows())
		assert.Equal(t, 10, bow.NumCols())
		assert.Equal(t, Int64, bow.GetType(0))

		bow2, err := bow.DropNil()
		assert.Nil(t, err)
		assert.Equal(t, bow, bow2)
		assert.True(t, bow2.Equal(bow), fmt.Sprintf("want %v\ngot %v", bow, bow2))
	})

	t.Run("with missing data", func(t *testing.T) {
		bow, err := NewGenBow(GenMissingData(true))
		assert.Nil(t, err)
		bow2, err := bow.DropNil()
		assert.Nil(t, err)
		assert.Less(t, bow2.NumRows(), bow.NumRows())
	})

	t.Run("float64 with first column sorted", func(t *testing.T) {
		bow, err := NewGenBow(GenRows(8), GenCols(2), GenDataType(Float64), GenRefCol(0, false))
		assert.Nil(t, err)

		assert.Equal(t, 8, bow.NumRows())
		assert.Equal(t, 2, bow.NumCols())
		assert.Equal(t, Float64, bow.GetType(0))
		assert.Equal(t, Float64, bow.GetType(1))
		sorted := bow.IsColSorted(0)
		assert.True(t, sorted)
	})

	t.Run("descending sort on last column", func(t *testing.T) {
		bow, err := NewGenBow(GenRefCol(9, true))
		assert.Nil(t, err)
		sorted := bow.IsColSorted(9)
		assert.True(t, sorted)
	})

	t.Run("custom names and types", func(t *testing.T) {
		bow, err := NewGenBow(
			GenCols(4),
			GenColNames([]string{"A", "B", "C", "D"}),
			GenDataTypes([]Type{Int64, Float64, String, Bool}),
			GenRefCol(0, true),
		)
		assert.Nil(t, err)

		sorted := bow.IsColSorted(0)
		assert.True(t, sorted)

		n, _ := bow.GetName(0)
		assert.Equal(t, "A", n)
		n, _ = bow.GetName(1)
		assert.Equal(t, "B", n)
		n, _ = bow.GetName(2)
		assert.Equal(t, "C", n)
		n, _ = bow.GetName(3)
		assert.Equal(t, "D", n)

		assert.Equal(t, Int64, bow.GetType(0))
		assert.Equal(t, Float64, bow.GetType(1))
		assert.Equal(t, String, bow.GetType(2))
		assert.Equal(t, Bool, bow.GetType(3))
	})
}
