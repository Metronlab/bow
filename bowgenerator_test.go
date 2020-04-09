package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerator(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		bow, err := NewRandomBow()
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
		bow, err := NewRandomBow(MissingData(true))
		assert.Nil(t, err)
		bow2, err := bow.DropNil()
		assert.Nil(t, err)
		assert.Less(t, bow2.NumRows(), bow.NumRows())
	})

	t.Run("float64 with first column sorted", func(t *testing.T) {
		bow, err := NewRandomBow(Rows(100), Cols(50), DataType(Float64), RefCol(0))
		assert.Nil(t, err)
		assert.Equal(t, 100, bow.NumRows())
		assert.Equal(t, 50, bow.NumCols())
		assert.Equal(t, Float64, bow.GetType(0))
		sorted, err := bow.IsColSorted(0)
		assert.NoError(t, err)
		assert.True(t, sorted)
	})

	t.Run("descending sort on last column", func(t *testing.T) {
		bow, err := NewRandomBow(RefCol(9), DescSort(true))
		assert.Nil(t, err)
		sorted, err := bow.IsColSorted(9)
		assert.NoError(t, err)
		assert.True(t, sorted)
	})

	t.Run("unsupported data type", func(t *testing.T) {
		_, err := NewRandomBow(DataType(String))
		assert.Error(t, err)
	})
}
