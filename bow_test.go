package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBowEmpty(t *testing.T) {
	assert.Equal(t, 0, NewBowEmpty().NumRows())
	assert.Equal(t, 0, NewBowEmpty().NumCols())
}

func TestNewBowFromColumnBasedInterface(t *testing.T) {
	t.Run("nil colTypes", func(t *testing.T) {
		b, err := NewBowFromColBasedInterfaces(
			[]string{"int", "float", "string", "bool"},
			nil,
			[][]interface{}{
				{10, 2},
				{10., 2.},
				{"hey", "ho"},
				{false, true},
			})
		require.NoError(t, err)

		expected, err := NewBowFromColBasedInterfaces(
			[]string{"int", "float", "string", "bool"},
			[]Type{Int64, Float64, String, Bool},
			[][]interface{}{
				{10, 2},
				{10., 2.},
				{"hey", "ho"},
				{false, true},
			})
		require.NoError(t, err)

		assert.Equal(t, b.String(), expected.String())
	})
}

func TestBow_SortByCol(t *testing.T) {
	t.Run("sorted", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{10, 2.4, 3.1},
				{11, 2.8, 5.9},
				{12, 2.9, 7.5},
				{13, 3.9, 13.4},
			})
		require.NoError(t, err)

		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, b.String(), sorted.String())
	})

	t.Run("simple unsorted - all types", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "i", "f", "b", "s"},
			[]Type{Int64, Int64, Float64, Bool, String},
			[][]interface{}{
				{10, 2, 3.1, true, "ho"},
				{11, 2, 5.9, false, "la"},
				{13, 3, 13.4, true, "tal"},
				{12, 2, 7.5, false, "que"},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "i", "f", "b", "s"},
			[]Type{Int64, Int64, Float64, Bool, String},
			[][]interface{}{
				{10, 2, 3.1, true, "ho"},
				{11, 2, 5.9, false, "la"},
				{12, 2, 7.5, false, "que"},
				{13, 3, 13.4, true, "tal"},
			})
		require.NoError(t, err)
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("simple unsorted different cols", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "time"},
			[]Type{Float64, Float64, Int64},
			[][]interface{}{
				{2.4, 3.1, 10},
				{2.8, 5.9, 11},
				{3.9, 13.4, 13},
				{2.9, 7.5, 12},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "time"},
			[]Type{Float64, Float64, Int64},
			[][]interface{}{
				{2.4, 3.1, 10},
				{2.8, 5.9, 11},
				{2.9, 7.5, 12},
				{3.9, 13.4, 13},
			})
		require.NoError(t, err)
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("simple unsorted with nil values", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{10, 2.4, nil},
				{11, 2.8, 5.9},
				{13, nil, 13.4},
				{12, 2.9, 7.5},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{10, 2.4, nil},
				{11, 2.8, 5.9},
				{12, 2.9, 7.5},
				{13, nil, 13.4},
			})
		require.NoError(t, err)
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("sorted in desc order", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{13, 3.9, 13.4},
				{12, 2.9, 7.5},
				{11, 2.8, 5.9},
				{10, 2.4, 3.1},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{10, 2.4, 3.1},
				{11, 2.8, 5.9},
				{12, 2.9, 7.5},
				{13, 3.9, 13.4},
			})
		require.NoError(t, err)
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("duplicate values - sort by column", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{13, 3.9, 13.4},
				{12, 2.9, 7.5},
				{12, 2.8, 5.9},
				{10, 2.4, 3.1},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{10, 2.4, 3.1},
				{12, 2.9, 7.5},
				{12, 2.8, 5.9},
				{13, 3.9, 13.4},
			})
		require.NoError(t, err)
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("nil values - sort by column", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{13, 3.9, 13.4},
				{12, 2.9, 7.5},
				{nil, 2.8, 5.9},
				{10, 2.4, 3.1},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{nil, 2.8, 5.9},
				{10, 2.4, 3.1},
				{12, 2.9, 7.5},
				{13, 3.9, 13.4},
			})
		require.NoError(t, err)
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("no rows", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a"},
			[]Type{Int64, Float64},
			[][]interface{}{})
		require.NoError(t, err)
		expected := b
		sorted, err := b.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})

	t.Run("with metadata", func(t *testing.T) {
		b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 3, 2}, nil),
			NewSeries("value", Float64, []float64{.1, .3, .2}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 2, 3}, nil),
			NewSeries("value", Float64, []float64{.1, .2, .3}, nil),
		)
		require.NoError(t, err)

		sorted, err := b.SortByCol("time")
		assert.NoError(t, err)

		assert.Equal(t, expected.String(), sorted.String())
	})

	t.Run("ERR: empty bow", func(t *testing.T) {
		b := NewBowEmpty()
		_, err := b.SortByCol("time")
		assert.Error(t, err)
	})

	t.Run("ERR: missing column", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"other", "a", "b"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{13, 3.9, 13.4},
				{12, 2.9, 7.5},
				{11, 2.8, 5.9},
				{10, 2.4, 3.1},
			})
		require.NoError(t, err)
		_, err = b.SortByCol("time")
		assert.Error(t, err)
	})

	t.Run("ERR: unsupported type - sort by column", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b"},
			[]Type{Float64, Float64, Float64},
			[][]interface{}{
				{13., 3.9, 13.4},
				{12., 2.9, 7.5},
				{11., 2.8, 5.9},
				{10., 2.4, 3.1},
			})
		require.NoError(t, err)
		_, err = b.SortByCol("time")
		assert.Error(t, err)
	})
}

func TestBow_Slice(t *testing.T) {
	origin, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", Int64, []int64{1, 2, 3}, nil),
		NewSeries("value", Float64, []float64{.1, .2, .3}, nil),
	)
	require.NoError(t, err)

	// begin
	expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", Int64, []int64{1}, nil),
		NewSeries("value", Float64, []float64{.1}, nil),
	)
	require.NoError(t, err)

	res := origin.Slice(0, 1)
	assert.True(t, expected.Equal(res),
		fmt.Sprintf("Have:\n%v,\nExpect:\n%v", res, expected))

	// end
	expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", Int64, []int64{2, 3}, nil),
		NewSeries("value", Float64, []float64{.2, .3}, nil),
	)
	require.NoError(t, err)

	res = origin.Slice(1, 3)
	assert.True(t, expected.Equal(res),
		fmt.Sprintf("Have:\n%v,\nExpect:\n%v", res, expected))

	// empty on already sliced bow (recursive test)
	expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", Int64, []int64{}, nil),
		NewSeries("value", Float64, []float64{}, nil),
	)
	require.NoError(t, err)

	res = res.Slice(1, 1)
	assert.True(t, expected.Equal(res),
		fmt.Sprintf("Have:\n%v,\nExpect:\n%v", res, expected))
}

func TestBow_Select(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		b := NewBowEmpty()
		newBow, err := b.Select()
		assert.NoError(t, err)
		assert.True(t, b.Equal(newBow),
			fmt.Sprintf("Have:\n%v,\nExpect:\n%v", newBow, b))
	})

	b, err := NewBowFromRowBasedInterfaces(
		[]string{"time", "a", "b"},
		[]Type{Int64, Float64, Float64},
		[][]interface{}{
			{13, 3.9, 13.4},
			{12, 2.9, 7.5},
			{11, 2.8, 5.9},
			{10, 2.4, 3.1},
		})
	require.NoError(t, err)

	t.Run("without colNames", func(t *testing.T) {
		expected := NewBowEmpty()

		newBow, err := b.Select()
		assert.NoError(t, err)
		assert.True(t, expected.Equal(newBow),
			fmt.Sprintf("Have:\n%v,\nExpect:\n%v", newBow, expected))
	})

	t.Run("valid", func(t *testing.T) {
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{13, 3.9},
				{12, 2.9},
				{11, 2.8},
				{10, 2.4},
			})
		require.NoError(t, err)

		newBow, err := b.Select("time", "a")
		assert.NoError(t, err)
		assert.True(t, expected.Equal(newBow),
			fmt.Sprintf("Have:\n%v,\nExpect:\n%v", newBow, expected))
	})

	t.Run("wrong colNames", func(t *testing.T) {
		newBow, err := b.Select("time", "unknown")
		assert.Nil(t, newBow)
		assert.Error(t, err)
	})

	t.Run("with metadata", func(t *testing.T) {
		b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 2, 3}, []bool{true, false, true}),
			NewSeries("value", Float64, []float64{1, 2, 3}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 2, 3}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		res, err := b.Select("time")
		require.NoError(t, err)

		assert.Equal(t, expected.String(), res.String())
	})
}

func TestBow_DropNil(t *testing.T) {
	filledBow, _ := NewBowFromColBasedInterfaces(
		[]string{"a", "b", "c"},
		[]Type{Int64, Int64, Int64},
		[][]interface{}{
			{100, 200, 300, 400},
			{110, 220, 330, 440},
			{111, 222, 333, 444},
		})
	holedBow, _ := NewBowFromColBasedInterfaces(
		[]string{"a", "b", "c"},
		[]Type{Int64, Int64, Int64},
		[][]interface{}{
			{nil, 200, 300, 400},
			{110, nil, 330, 440},
			{111, nil, 333, nil},
		})

	t.Run("empty bow", func(t *testing.T) {
		b, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{
				{},
			})
		compacted, err := b.DropNil()
		expected, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{
				{},
			})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("unchanged without nil", func(t *testing.T) {
		compacted, err := filledBow.DropNil()
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, compacted))
	})

	t.Run("drop default", func(t *testing.T) {
		compactedDefault, err := holedBow.DropNil()
		assert.Nil(t, err)
		compactedAll, err := holedBow.DropNil("b", "c", "a")
		assert.Nil(t, err)
		assert.True(t, compactedDefault.Equal(compactedAll),
			fmt.Sprintf("default %v\nall %v", compactedDefault, compactedAll))
	})

	t.Run("drop on all columns", func(t *testing.T) {
		compacted, err := holedBow.DropNil()
		expected, _ := NewBowFromColBasedInterfaces(
			[]string{"a", "b", "c"},
			[]Type{Int64, Int64, Int64},
			[][]interface{}{
				{300},
				{330},
				{333},
			})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("drop on one column", func(t *testing.T) {
		compacted, err := holedBow.DropNil("b")
		expected, _ := NewBowFromColBasedInterfaces(
			[]string{"a", "b", "c"},
			[]Type{Int64, Int64, Int64},
			[][]interface{}{
				{nil, 300, 400},
				{110, 330, 440},
				{111, 333, nil},
			})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("drop consecutively at start/middle/end", func(t *testing.T) {
		b, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{
				{nil, nil, 1, nil, nil, 2, nil, nil},
			})
		compacted, err := b.DropNil()
		expected, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{
				{1, 2},
			})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("with metadata", func(t *testing.T) {
		b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 2, 3}, []bool{true, false, true}),
			NewSeries("value", Float64, []float64{1, 2, 3}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 3}, nil),
			NewSeries("value", Float64, []float64{1, 3}, nil),
		)
		require.NoError(t, err)

		res, err := b.DropNil()
		require.NoError(t, err)

		assert.Equal(t, expected.String(), res.String())
	})
}

func TestAddCols(t *testing.T) {
	bow1, err := NewBowFromRowBasedInterfaces(
		[]string{"time", "a", "b"},
		[]Type{Int64, Float64, Float64},
		[][]interface{}{
			{1, 1.1, 2.1},
			{2, 1.2, 2.2},
			{3, 1.3, 2.3},
			{4, 1.4, 2.4},
		})
	require.NoError(t, err)
	serieC := NewSeries("c", Int64, []int64{1, 2, 3, 4}, nil)
	serieD := NewSeries("d", String, []string{"one", "two", "three", "four"}, nil)
	serieE := NewSeries("e", Bool, []bool{true, false, true, false}, nil)

	t.Run("empty", func(t *testing.T) {
		b := NewBowEmpty()
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"c", "d"},
			[]Type{Int64, String},
			[][]interface{}{
				{1, "one"},
				{2, "two"},
				{3, "three"},
				{4, "four"},
			})
		require.NoError(t, err)

		newBow, err := b.AddCols(serieC, serieD)
		require.NoError(t, err)
		assert.True(t, newBow.Equal(expected), "expected: %q have: %q", expected, newBow)
	})

	t.Run("valid series", func(t *testing.T) {
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "a", "b", "c", "d", "e"},
			[]Type{Int64, Float64, Float64, Int64, String, Bool},
			[][]interface{}{
				{1, 1.1, 2.1, 1, "one", true},
				{2, 1.2, 2.2, 2, "two", false},
				{3, 1.3, 2.3, 3, "three", true},
				{4, 1.4, 2.4, 4, "four", false},
			})
		require.NoError(t, err)

		newBow, err := bow1.AddCols(serieC, serieD, serieE)
		require.NoError(t, err)
		assert.True(t, newBow.Equal(expected), "expected: %q have: %q", expected, newBow)
	})

	t.Run("column name already exists", func(t *testing.T) {
		_, err := bow1.AddCols(NewSeries("a", Int64, []int64{1, 2, 3, 4}, nil))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("duplicate column name", func(t *testing.T) {
		_, err := bow1.AddCols(serieC, serieC)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("no series", func(t *testing.T) {
		newBow, err := bow1.AddCols()
		require.NoError(t, err)
		assert.True(t, newBow.Equal(bow1), "expected: %q have: %q", bow1, newBow)
	})
}
