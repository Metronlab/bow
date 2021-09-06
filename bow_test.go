package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkBowsDirPath = "benchmarks/"
)

func ExpectEqual(t *testing.T, expect, have Bow) {
	assert.True(t, expect.Equal(have), "expect:\n%shave:\n%s", expect, have)
}

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
			[]Type{Int64, Float64, String, Boolean},
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

func TestBow_NewSlice(t *testing.T) {
	origin, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", []int64{1, 2, 3}, nil),
		NewSeries("value", []float64{.1, .2, .3}, nil),
	)
	require.NoError(t, err)

	// begin
	expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", []int64{1}, nil),
		NewSeries("value", []float64{.1}, nil),
	)
	require.NoError(t, err)

	res := origin.NewSlice(0, 1)
	assert.True(t, expected.Equal(res),
		fmt.Sprintf("Have:\n%v,\nExpect:\n%v", res, expected))

	// end
	expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", []int64{2, 3}, nil),
		NewSeries("value", []float64{.2, .3}, nil),
	)
	require.NoError(t, err)

	res = origin.NewSlice(1, 3)
	assert.True(t, expected.Equal(res),
		fmt.Sprintf("Have:\n%v,\nExpect:\n%v", res, expected))

	// empty on already sliced bow (recursive test)
	expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("time", []int64{}, nil),
		NewSeries("value", []float64{}, nil),
	)
	require.NoError(t, err)

	res = res.NewSlice(1, 1)
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
			NewSeries("time", []int64{1, 2, 3}, []bool{true, false, true}),
			NewSeries("value", []float64{1, 2, 3}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", []int64{1, 2, 3}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		res, err := b.Select("time")
		require.NoError(t, err)

		assert.Equal(t, expected.String(), res.String())
	})
}

func TestBow_DropNils(t *testing.T) {
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
		compacted, err := b.DropNils()
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
		compacted, err := filledBow.DropNils()
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, compacted))
	})

	t.Run("drop default", func(t *testing.T) {
		compactedDefault, err := holedBow.DropNils()
		assert.Nil(t, err)
		compactedAll, err := holedBow.DropNils("b", "c", "a")
		assert.Nil(t, err)
		assert.True(t, compactedDefault.Equal(compactedAll),
			fmt.Sprintf("default %v\nall %v", compactedDefault, compactedAll))
	})

	t.Run("drop on all columns", func(t *testing.T) {
		compacted, err := holedBow.DropNils()
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
		compacted, err := holedBow.DropNils("b")
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
		compacted, err := b.DropNils()
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
			NewSeries("time", []int64{1, 2, 3}, []bool{true, false, true}),
			NewSeries("value", []float64{1, 2, 3}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", []int64{1, 3}, nil),
			NewSeries("value", []float64{1, 3}, nil),
		)
		require.NoError(t, err)

		res, err := b.DropNils()
		require.NoError(t, err)

		assert.Equal(t, expected.String(), res.String())
	})
}

func TestBow_AddCols(t *testing.T) {
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
	serieC := NewSeries("c", []int64{1, 2, 3, 4}, nil)
	serieD := NewSeries("d", []string{"one", "two", "three", "four"}, nil)
	serieE := NewSeries("e", []bool{true, false, true, false}, nil)

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
			[]Type{Int64, Float64, Float64, Int64, String, Boolean},
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
		_, err := bow1.AddCols(NewSeries("a", []int64{1, 2, 3, 4}, nil))
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
