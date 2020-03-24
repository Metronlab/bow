package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBow_FillIsColSorted(t *testing.T) {
	var err error
	intBobow, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
		{-2, 1, nil, nil, -8},
		{0, nil, 3, 4, 0},
		{1, nil, nil, 120, nil},
		{10, 4, 10, 10, -5},
		{13, nil, nil, nil, nil},
		{20, 6, 30, 400, -10},
	})

	t.Run("column a sorted", func(t *testing.T) {
		err = isColSorted(intBobow, 0, Int64)
		assert.Nil(t, err)
	})

	t.Run("column b sorted", func(t *testing.T) {
		err = isColSorted(intBobow, 1, Int64)
		assert.Nil(t, err)
	})

	t.Run("column c sorted", func(t *testing.T) {
		err = isColSorted(intBobow, 2, Int64)
		assert.Nil(t, err)
	})

	t.Run("column d unsorted", func(t *testing.T) {
		err = isColSorted(intBobow, 3, Int64)
		assert.Error(t, err)
	})

	t.Run("column e unsorted", func(t *testing.T) {
		err = isColSorted(intBobow, 4, Int64)
		assert.Error(t, err)
	})

	t.Run("int64 random bow with missing data asc sorted", func(t *testing.T) {
		bobow, err := NewRandomBow(1000, 1000, Int64, true, 1, true)
		assert.Nil(t, err)
		err = isColSorted(bobow, 1, Int64)
		assert.Nil(t, err)
	})

	t.Run("int64 random bow with missing data desc sorted", func(t *testing.T) {
		bobow, err := NewRandomBow(1000, 1000, Int64, true, 1, false)
		assert.Nil(t, err)
		err = isColSorted(bobow, 1, Int64)
		assert.Nil(t, err)
	})

	t.Run("float64 random bow with missing data asc sorted", func(t *testing.T) {
		bobow, err := NewRandomBow(1000, 1000, Float64, true, 0, true)
		assert.Nil(t, err)
		err = isColSorted(bobow, 0, Float64)
		assert.Nil(t, err)
	})

	t.Run("float64 random bow with missing data desc sorted", func(t *testing.T) {
		bobow, err := NewRandomBow(1000, 1000, Float64, true, 0, false)
		assert.Nil(t, err)
		err = isColSorted(bobow, 0, Float64)
		assert.Nil(t, err)
	})

	floatBobow, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
		{-2.0, 1.0, nil, nil, -8.0},
		{0.0, nil, 3.0, 4.0, 0.0},
		{1.0, nil, nil, 120.0, nil},
		{10.0, 4.0, 10.0, 10.0, -5.0},
		{13.0, nil, nil, nil, nil},
		{20.0, 6.0, 30.0, 400.0, -10.0},
	})

	t.Run("column a sorted", func(t *testing.T) {
		err = isColSorted(floatBobow, 0, Float64)
		assert.Nil(t, err)
	})

	t.Run("column b sorted", func(t *testing.T) {
		err = isColSorted(floatBobow, 1, Float64)
		assert.Nil(t, err)
	})

	t.Run("column c sorted", func(t *testing.T) {
		err = isColSorted(floatBobow, 2, Float64)
		assert.Nil(t, err)
	})

	t.Run("column d unsorted", func(t *testing.T) {
		err = isColSorted(floatBobow, 3, Float64)
		assert.Error(t, err)
	})

	t.Run("column e unsorted", func(t *testing.T) {
		err = isColSorted(floatBobow, 4, Float64)
		assert.Error(t, err)
	})
}

// TODO: need to add more tests for FillLinear
func TestBow_FillLinear(t *testing.T) {
	holedInt, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
		{-2, 1, nil, nil, -8},
		{0, nil, 3, 4, 0},
		{1, nil, nil, 120, nil},
		{10, 4, 10, 10, -5},
		{13, nil, nil, nil, nil},
		{20, 6, 30, 400, -10},
	})

	t.Run("int64 fill linear on first column", func(t *testing.T) {
		filled, err := holedInt.FillLinear("a", "b")
		expected, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{-2, 1, nil, nil, -8},
			{0, 1, 3, 4, 0},
			{1, 1, nil, 120, nil},
			{10, 4, 10, 10, -5},
			{13, 4, nil, nil, nil},
			{20, 6, 30, 400, -10},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})
	holedFloat, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
		{20.0, 6.0, 30.0, 400.0, -10.0},
		{13.0, nil, nil, nil, nil},
		{10.0, 4.0, 10.0, 10.0, -5.0},
		{1.0, nil, nil, 120.0, nil},
		{0.0, nil, 3.0, 4.0, 0.0},
		{-2.0, 1.0, nil, nil, -8.0},
	})

	t.Run("float64 fill linear on first column", func(t *testing.T) {
		filled, err := holedFloat.FillLinear("a", "b")
		expected, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, 4.6, nil, nil, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, 1.75, nil, 120.0, nil},
			{0.0, 1.5, 3.0, 4.0, 0.0},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("float64 fill linear on second column", func(t *testing.T) {
		filled, err := holedFloat.FillLinear("a", "c")
		expected, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{20.0, 6.0, 30.0, 400.0, -10.0},
			{13.0, nil, 16.0, nil, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{1.0, nil, 3.7, 120.0, nil},
			{0.0, nil, 3.0, 4.0, 0.0},
			{-2.0, 1.0, nil, nil, -8.0},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	unsortedRefCol, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
		{-2, 1, nil, nil, -8},
		{0, nil, 3, 4, 0},
		{1, nil, nil, 120, nil},
		{14, 4, 10, 10, -5},
		{13, nil, nil, nil, nil},
		{20, 6, 30, 400, -10},
	})

	t.Run("error unsorted refCol a", func(t *testing.T) {
		filled, err := unsortedRefCol.FillLinear("a", "b")
		assert.Nil(t, filled)
		assert.Error(t, err)
	})

	t.Run("error unsorted refCol e", func(t *testing.T) {
		filled, err := unsortedRefCol.FillLinear("e", "b")
		assert.Nil(t, filled)
		assert.Error(t, err)
	})
}

func TestBow_FillMean(t *testing.T) {
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
		filled, err := b.FillMean()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, filled))
	})

	t.Run("int64 Bow unchanged without nil", func(t *testing.T) {
		filled, err := filledInt.FillMean()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledInt),
			fmt.Sprintf("want %v\ngot %v", filledInt, filled))
	})

	t.Run("float64 Bow unchanged without nil", func(t *testing.T) {
		filled, err := filledFloat.FillMean()
		assert.Nil(t, err)
		assert.True(t, filled.Equal(filledFloat),
			fmt.Sprintf("want %v\ngot %v", filledFloat, filled))
	})

	t.Run("compare default fill mean with all columns at random order", func(t *testing.T) {
		filledDefault, err := holedInt.FillMean()
		assert.Nil(t, err)
		filledAll, err := holedInt.FillMean("b", "c", "d", "a")
		assert.Nil(t, err)
		assert.True(t, filledDefault.Equal(filledAll),
			fmt.Sprintf("default %v\nall %v", filledDefault, filledAll))
	})

	t.Run("unexisting column name", func(t *testing.T) {
		filled, err := holedInt.FillMean("b", "c", "x", "a")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("int64 fill mean on all columns", func(t *testing.T) {
		filled, err := holedInt.FillMean()
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

	t.Run("int64 fill mean on one column", func(t *testing.T) {
		filled, err := holedInt.FillMean("b")
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

	t.Run("float64 fill mean on all columns", func(t *testing.T) {
		filled, err := holedFloat.FillMean()
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

	t.Run("float64 fill mean on one column", func(t *testing.T) {
		filled, err := holedFloat.FillMean("b")
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

	t.Run("non-numeric type fill mean on one column", func(t *testing.T) {
		filled, err := holedString.FillMean("b")
		assert.Nil(t, filled)
		assert.NotNil(t, err)
	})

	t.Run("non-numeric type fill mean on all column", func(t *testing.T) {
		filled, err := holedString.FillMean()
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
