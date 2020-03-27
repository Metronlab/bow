package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBow_FillPrevious2(t *testing.T) {
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
		filled, err := b.FillPrevious2()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("unchanged without nil", func(t *testing.T) {
		filled, err := filledBow.FillPrevious2()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, filled))
	})

	t.Run("compare default fill previous with all columns at random order", func(t *testing.T) {
		filledDefault, err := holedBow.FillPrevious2()
		assert.Nil(t, err)
		filledAll, err := holedBow.FillPrevious2("b", "c", "d", "a")
		assert.Nil(t, err)
		assert.True(t, filledDefault.Equal(filledAll),
			fmt.Sprintf("default %v\nall %v", filledDefault, filledAll))
	})

	t.Run("wrong column name", func(t *testing.T) {
		filled, err := holedBow.FillPrevious2("b", "c", "x", "a")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("fill previous on all columns", func(t *testing.T) {
		filled, err := holedBow.FillPrevious2()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 110, 330, 440},
			{111, 111, 333, 333},
			{113, 113, 113, 113},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("fill previous on one column", func(t *testing.T) {
		filled, err := holedBow.FillPrevious2("b")
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 110, 330, 440},
			{111, nil, 333, nil},
			{113, nil, nil, 113},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})
}
