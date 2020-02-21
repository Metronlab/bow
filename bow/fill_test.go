package bow

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBow_FillPrevious(t *testing.T) {
	filledBow, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
		{100, 200, 300, 400},
		{110, 220, 330, 440},
		{111, 222, 333, 444},
		{113, 113, 113, 113},
	})

	holedBow, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
		{nil, 200, 300, 400},
		{110, nil, 330, 440},
		{111, nil, 333, nil},
		{113, nil, nil, 113},
	})

	t.Run("empty bow", func(t *testing.T) {
		b, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		compacted, err := b.FillPrevious()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("unchanged without nil", func(t *testing.T) {
		compacted, err := filledBow.FillPrevious()
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, compacted))
	})

	t.Run("compare default fill previous with all columns at random order", func(t *testing.T) {
		compactedDefault, err := holedBow.FillPrevious()
		assert.Nil(t, err)
		compactedAll, err := holedBow.FillPrevious("b", "c", "d", "a")
		assert.Nil(t, err)
		assert.True(t, compactedDefault.Equal(compactedAll),
			fmt.Sprintf("default %v\nall %v", compactedDefault, compactedAll))
	})

	t.Run("wrong column name", func(t *testing.T) {
		nilBow, err := holedBow.FillPrevious("b", "c", "x", "a")
		assert.Nil(t, nilBow)
		assert.NotNil(t, err)
	})

	t.Run("fill previous on all columns", func(t *testing.T) {
		compacted, err := holedBow.FillPrevious()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 110, 330, 440},
			{111, 111, 333, 333},
			{113, 113, 113, 113},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("fill previous on one column", func(t *testing.T) {
		compacted, err := holedBow.FillPrevious("b")
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 110, 330, 440},
			{111, nil, 333, nil},
			{113, nil, nil, 113},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})
}
