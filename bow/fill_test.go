package bow

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBow_FillLinear(t *testing.T) {
	filledInt, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
		{100, 200, 300, 400},
		{110, 220, 330, 440},
		{111, 222, 333, 444},
		{113, 113, 113, 140},
	})

	holedInt, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
		{nil, 200, 300, 400},
		{110, nil, 330, 440},
		{111, nil, 333, nil},
		{113, nil, nil, 140},
	})

	filledFloat, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Float64, Float64, Float64, Float64}, [][]interface{}{
		{10.0, 20.0, 30.0, 40.0},
		{11.0, 22.0, 33.0, 44.0},
		{11.1, 22.2, 33.3, 44.4},
		{11.3, 11.3, 11.3, 14.0},
	})

	holedFloat, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Float64, Float64, Float64, Float64}, [][]interface{}{
		{nil, 20.0, 30.0, 40.0},
		{11.0, nil, 33.0, 44.0},
		{11.1, nil, 33.3, nil},
		{11.3, nil, nil, 14.0},
	})

	holedString, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Float64, String, Float64, Float64}, [][]interface{}{
		{nil, 20.0, 30.0, 40.0},
		{"this", "is", "an", "error"},
		{11.1, nil, 33.3, nil},
		{11.3, nil, nil, 14.0},
	})

	t.Run("empty bow", func(t *testing.T) {
		b, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		filled, err := b.FillLinear()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("int64 Bow unchanged without nil", func(t *testing.T) {
		filled, err := filledInt.FillLinear()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledInt),
			fmt.Sprintf("want %v\ngot %v", filledInt, filled))
	})

	t.Run("float64 Bow unchanged without nil", func(t *testing.T) {
		filled, err := filledFloat.FillLinear()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledFloat),
			fmt.Sprintf("want %v\ngot %v", filledFloat, filled))
	})

	t.Run("compare default fill linear with all columns at random order", func(t *testing.T) {
		filledDefault, err := holedInt.FillLinear()
		assert.Nil(t, err)
		filledAll, err := holedInt.FillLinear("b", "c", "d", "a")
		assert.Nil(t, err)
		assert.True(t, filledDefault.Equal(filledAll),
			fmt.Sprintf("default %v\nall %v", filledDefault, filledAll))
	})

	t.Run("unexisting column name", func(t *testing.T) {
		filled, err := holedInt.FillLinear("b", "c", "x", "a")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("int64 fill linear on all columns", func(t *testing.T) {
		filled, err := holedInt.FillLinear()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 220, 330, 440},
			{111, 222, 333, nil},
			{113, 126, 126, 140},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("int64 fill linear on one column", func(t *testing.T) {
		filled, err := holedInt.FillLinear("b")
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 220, 330, 440},
			{111, nil, 333, nil},
			{113, nil, nil, 140},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("float64 fill linear on all columns", func(t *testing.T) {
		filled, err := holedFloat.FillLinear()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Float64, Float64, Float64, Float64}, [][]interface{}{
			{nil, 20.0, 30.0, 40.0},
			{11.0, 22.0, 33.0, 44.0},
			{11.1, 22.2, 33.3, nil},
			{11.3, 12.65, 12.65, 14.0},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("float64 fill linear on one column", func(t *testing.T) {
		filled, err := holedFloat.FillLinear("b")
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Float64, Float64, Float64, Float64}, [][]interface{}{
			{nil, 20.0, 30.0, 40.0},
			{11.0, 22.0, 33.0, 44.0},
			{11.1, nil, 33.3, nil},
			{11.3, nil, nil, 14.0},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("non-numeric type fill linear on one column", func(t *testing.T) {
		filled, err := holedString.FillLinear("b")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("non-numeric type fill linear on all column", func(t *testing.T) {
		filled, err := holedString.FillLinear()
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})
}

func TestBow_FillNext(t *testing.T) {
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
		filled, err := b.FillNext()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("unchanged without nil", func(t *testing.T) {
		filled, err := filledBow.FillNext()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, filled))
	})

	t.Run("compare default fill previous with all columns at random order", func(t *testing.T) {
		filledDefault, err := holedBow.FillNext()
		assert.Nil(t, err)
		filledAll, err := holedBow.FillNext("b", "c", "d", "a")
		assert.Nil(t, err)
		assert.True(t, filledDefault.Equal(filledAll),
			fmt.Sprintf("default %v\nall %v", filledDefault, filledAll))
	})

	t.Run("wrong column name", func(t *testing.T) {
		filled, err := holedBow.FillNext("b", "c", "x", "a")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("fill previous on all columns", func(t *testing.T) {
		filled, err := holedBow.FillNext()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{200, 200, 300, 400},
			{110, 330, 330, 440},
			{111, 333, 333, nil},
			{113, 113, 113, 113},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("fill previous on one column", func(t *testing.T) {
		filled, err := holedBow.FillNext("b")
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c", "d"}, []Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{nil, 200, 300, 400},
			{110, 330, 330, 440},
			{111, nil, 333, nil},
			{113, nil, nil, 113},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})
}

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
		filled, err := b.FillPrevious()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("unchanged without nil", func(t *testing.T) {
		filled, err := filledBow.FillPrevious()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, filled))
	})

	t.Run("compare default fill previous with all columns at random order", func(t *testing.T) {
		filledDefault, err := holedBow.FillPrevious()
		assert.Nil(t, err)
		filledAll, err := holedBow.FillPrevious("b", "c", "d", "a")
		assert.Nil(t, err)
		assert.True(t, filledDefault.Equal(filledAll),
			fmt.Sprintf("default %v\nall %v", filledDefault, filledAll))
	})

	t.Run("wrong column name", func(t *testing.T) {
		filled, err := holedBow.FillPrevious("b", "c", "x", "a")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("fill previous on all columns", func(t *testing.T) {
		filled, err := holedBow.FillPrevious()
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
		filled, err := holedBow.FillPrevious("b")
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
