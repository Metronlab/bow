package bow

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFill(t *testing.T) {
	holedInt, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
		{20, 6, 30, 400, -10},
		{13, nil, nil, nil, nil},
		{10, 4, 10, 10, -5},
		{1, nil, nil, 120, nil},
		{0, nil, 3, 4, 0},
		{nil, nil, nil, nil, nil},
		{-2, 1, nil, nil, -8},
	})

	t.Run("Linear int64 refCol a toFill b (desc)", func(t *testing.T) {
		filled, err := holedInt.FillLinear("a", "b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 5, nil, nil, nil},
			{10, 4, 10, 10, -5},
			{1, 2, nil, 120, nil},
			{0, 2, 3, 4, 0},
			{nil, nil, nil, nil, nil},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Linear int64 refCol a toFill e (asc)", func(t *testing.T) {
		filled, err := holedInt.FillLinear("a", "e")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, nil, nil, nil, -7},
			{10, 4, 10, 10, -5},
			{1, nil, nil, 120, -1},
			{0, nil, 3, 4, 0},
			{nil, nil, nil, nil, nil},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Mean int64 toFill b", func(t *testing.T) {
		filled, err := holedInt.FillMean("b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 5, nil, nil, nil},
			{10, 4, 10, 10, -5},
			{1, 3, nil, 120, nil},
			{0, 3, 3, 4, 0},
			{nil, 3, nil, nil, nil},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Mean int64 toFill all columns", func(t *testing.T) {
		filled, err := holedInt.FillMean()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 5, 20, 205, -8},
			{10, 4, 10, 10, -5},
			{1, 3, 7, 120, -3},
			{0, 3, 3, 4, 0},
			{-1, 3, nil, nil, -4},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Next int64 toFill b", func(t *testing.T) {
		filled, err := holedInt.FillNext("b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 4, nil, nil, nil},
			{10, 4, 10, 10, -5},
			{1, 1, nil, 120, nil},
			{0, 1, 3, 4, 0},
			{nil, 1, nil, nil, nil},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Next int64 toFill all columns", func(t *testing.T) {
		filled, err := holedInt.FillNext()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 4, 10, 10, -5},
			{10, 4, 10, 10, -5},
			{1, 1, 3, 120, 0},
			{0, 1, 3, 4, 0},
			{-2, 1, nil, nil, -8},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Previous int64 toFill b", func(t *testing.T) {
		filled, err := holedInt.FillPrevious("b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 6, nil, nil, nil},
			{10, 4, 10, 10, -5},
			{1, 4, nil, 120, nil},
			{0, 4, 3, 4, 0},
			{nil, 4, nil, nil, nil},
			{-2, 1, nil, nil, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Previous int64 toFill all columns", func(t *testing.T) {
		filled, err := holedInt.FillPrevious()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{20, 6, 30, 400, -10},
			{13, 6, 30, 400, -10},
			{10, 4, 10, 10, -5},
			{1, 4, 10, 120, -5},
			{0, 4, 3, 4, 0},
			{0, 4, 3, 4, 0},
			{-2, 1, 3, 4, -8},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	holedFloat, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
		{20.0, 6.0, 30.0, 400.0, -10.0},
		{13.0, nil, nil, nil, nil},
		{10.0, 4.0, 10.0, 10.0, -5.0},
		{1.0, nil, nil, 120.0, nil},
		{0.0, nil, 3.0, 4.0, 0.0},
		{nil, nil, nil, nil, nil},
		{-2.0, 1.0, nil, nil, -8.0},
	})

	t.Run("Linear float64 refCol a toFill b (desc)", func(t *testing.T) {
		filled, err := holedFloat.FillLinear("a", "b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 4.6, nil, nil, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 1.75, nil, 120.0, nil},
			{0.0, 1.5, 3.0, 4.0, 0.0},
			{nil, nil, nil, nil, nil},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Linear float64 refCol a toFill e (asc)", func(t *testing.T) {
		filled, err := holedFloat.FillLinear("a", "e")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, nil, nil, nil, -6.5},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, nil, nil, 120.0, -0.5},
			{0.0, nil, 3.0, 4.0, 0.0},
			{nil, nil, nil, nil, nil},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Linear refCol not sorted", func(t *testing.T) {
		filled, err := holedFloat.FillLinear("d", "b")
		assert.Nil(t, filled)
		assert.Error(t, err)
		filled, err = holedInt.FillLinear("d", "b")
		assert.Nil(t, filled)
		assert.Error(t, err)
	})

	t.Run("Mean float64 toFill b", func(t *testing.T) {
		filled, err := holedFloat.FillMean("b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 5.0, nil, nil, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 2.5, nil, 120.0, nil},
			{0.0, 2.5, 3.0, 4.0, 0.0},
			{nil, 2.5, nil, nil, nil},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Mean float64 toFill all columns", func(t *testing.T) {
		filled, err := holedFloat.FillMean()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 5.0, 20.0, 205.0, -7.5},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 2.5, 6.5, 120.0, -2.5},
			{0.0, 2.5, 3.0, 4.0, 0.0},
			{-1.0, 2.5, nil, nil, -4.0},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Next float64 toFill b", func(t *testing.T) {
		filled, err := holedFloat.FillNext("b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 4.0, nil, nil, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 1.0, nil, 120.0, nil},
			{0.0, 1.0, 3.0, 4.0, 0.0},
			{nil, 1.0, nil, nil, nil},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Next float64 toFill all columns", func(t *testing.T) {
		filled, err := holedFloat.FillNext()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 4.0, 10.0, 10.0, -5.0},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 1.0, 3.0, 120.0, 0.0},
			{0.0, 1.0, 3.0, 4.0, 0.0},
			{-2.0, 1.0, nil, nil, -8.0},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Previous float64 toFill b", func(t *testing.T) {
		filled, err := holedFloat.FillPrevious("b")
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 6.0, nil, nil, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 4.0, nil, 120.0, nil},
			{0.0, 4.0, 3.0, 4.0, 0.0},
			{nil, 4.0, nil, nil, nil},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Previous float64 toFill all columns", func(t *testing.T) {
		filled, err := holedFloat.FillPrevious()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 6.0, 30.0, 400.0, -10.0},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 4.0, 10.0, 120.0, -5.0},
			{0.0, 4.0, 3.0, 4.0, 0.0},
			{0.0, 4.0, 3.0, 4.0, 0.0},
			{-2.0, 1.0, 3.0, 4.0, -8.0},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	nonNumeric, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Bool, String}, [][]interface{}{
		{20, nil, "dgr"},
		{13, false, "sfr"},
		{10, false, nil},
		{1, true, "hey"},
		{0, nil, "yop"},
		{-1, true, nil},
		{-2, false, "ioi"},
	})

	t.Run("Previous non numeric", func(t *testing.T) {
		filled, err := nonNumeric.FillPrevious()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Bool, String}, [][]interface{}{
			{20, nil, "dgr"},
			{13, false, "sfr"},
			{10, false, "sfr"},
			{1, true, "hey"},
			{0, true, "yop"},
			{-1, true, "yop"},
			{-2, false, "ioi"},
		})
		assert.NoError(t, err)
		assert.NotNil(t, filled)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Next non numeric", func(t *testing.T) {
		filled, err := nonNumeric.FillNext()
		assert.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Bool, String}, [][]interface{}{
			{20, false, "dgr"},
			{13, false, "sfr"},
			{10, false, "hey"},
			{1, true, "hey"},
			{0, true, "yop"},
			{-1, true, "ioi"},
			{-2, false, "ioi"},
		})
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected), fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("Mean non numeric", func(t *testing.T) {
		filled, err := nonNumeric.FillMean()
		assert.Error(t, err)
		assert.Nil(t, filled)
	})

	t.Run("Linear non numeric", func(t *testing.T) {
		filled, err := nonNumeric.FillLinear("a", "b")
		assert.Error(t, err)
		assert.Nil(t, filled)
	})
}
