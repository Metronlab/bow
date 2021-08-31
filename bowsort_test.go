package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	t.Run("unsorted with all types", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "i", "f", "b", "s"},
			[]Type{Int64, Int64, Float64, Boolean, String},
			[][]interface{}{
				{10, 2, 3.1, true, "ho"},
				{11, 2, 5.9, false, "la"},
				{13, 3, 13.4, true, "tal"},
				{12, 2, 7.5, false, "que"},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "i", "f", "b", "s"},
			[]Type{Int64, Int64, Float64, Boolean, String},
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

	t.Run("unsorted with different cols", func(t *testing.T) {
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

	t.Run("unsorted with nil values and all types", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "int", "float", "string", "bool"},
			[]Type{Int64, Int64, Float64, String, Boolean},
			[][]interface{}{
				{10, 5, nil, "bonjour", true},
				{11, 2, 56., "comment", false},
				{13, nil, 13.4, "allez", nil},
				{12, -1, nil, nil, false},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"time", "int", "float", "string", "bool"},
			[]Type{Int64, Int64, Float64, String, Boolean},
			[][]interface{}{
				{10, 5, nil, "bonjour", true},
				{11, 2, 56., "comment", false},
				{12, -1, nil, nil, false},
				{13, nil, 13.4, "allez", nil},
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

	t.Run("duplicate values in sort by column", func(t *testing.T) {
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

	t.Run("empty bow", func(t *testing.T) {
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
			NewSeries("time", []int64{1, 3, 2}, nil),
			NewSeries("value", []float64{.1, .3, .2}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", []int64{1, 2, 3}, nil),
			NewSeries("value", []float64{.1, .2, .3}, nil),
		)
		require.NoError(t, err)

		sorted, err := b.SortByCol("time")
		assert.NoError(t, err)

		assert.Equal(t, expected.String(), sorted.String())
	})

	t.Run("ERR: nil values in sort by column", func(t *testing.T) {
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
		_, err = b.SortByCol("time")
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

func BenchmarkBow_SortByCol(b *testing.B) {
	for rows := 10; rows <= 100000; rows *= 10 {
		data, err := NewBowFromParquet(fmt.Sprintf("%sbow1-%d-rows.parquet", benchmarkBowsDirPath, rows), false)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err = data.SortByCol("Int64_bow1")
				require.NoError(b, err)
			}
		})
	}
}
