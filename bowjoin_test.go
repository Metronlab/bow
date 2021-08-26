package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_OuterJoin(t *testing.T) {
	t.Run("two empty bows", func(t *testing.T) {
		b1 := NewBowEmpty()
		b2 := NewBowEmpty()
		expected := NewBowEmpty()

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("empty right bow", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		b2 := NewBowEmpty()

		expected := b1

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("empty left bow", func(t *testing.T) {
		b1 := NewBowEmpty()

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		expected := b2

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("left and right without rows", func(t *testing.T) {
		b1, err := NewBow(
			NewSeries("index1", []int64{}, nil),
			NewSeries("index2", []float64{}, nil),
			NewSeries("col1", []int64{}, nil),
		)
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col2"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		// Left bow without rows
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1", "col2"},
			[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
				{1, 1.1, nil, 1},
				{1, 1.1, nil, nil},
				{2, nil, nil, 3},
				{3, 3.3, nil, 4},
				{4, 4.4, nil, 5},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())

		// Right bow without rows
		expected, err = NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col2", "col1"},
			[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
				{1, 1.1, 1, nil},
				{1, 1.1, nil, nil},
				{2, nil, 3, nil},
				{3, 3.3, 4, nil},
				{4, 4.4, 5, nil},
			})
		require.NoError(t, err)

		result = b2.OuterJoin(b1)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("left and right bow without rows", func(t *testing.T) {
		b1, err := NewBow(
			NewSeries("index1", []int64{}, nil),
			NewSeries("index2", []float64{}, nil),
			NewSeries("col1", []int64{}, nil),
		)
		require.NoError(t, err)
		b2, err := NewBow(
			NewSeries("index1", []int64{}, nil),
			NewSeries("index2", []float64{}, nil),
			NewSeries("col2", []int64{}, nil),
		)
		require.NoError(t, err)
		expected, err := NewBow(
			NewSeries("index1", []int64{}, nil),
			NewSeries("index2", []float64{}, nil),
			NewSeries("col1", []int64{}, nil),
			NewSeries("col2", []int64{}, nil),
		)
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("timeSeries like", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces([]string{"a", "b"},
			[]Type{Int64, Int64}, [][]interface{}{
				{10, 0},
				{11, 1},
				{12, 2},
				{13, 3},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces([]string{"a", "c"},
			[]Type{Int64, Int64}, [][]interface{}{
				{11, 0},
				{12, 1},
				{13, 2},
				{14, 3},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"},
			[]Type{Int64, Int64, Int64}, [][]interface{}{
				{10, 0, nil},
				{11, 1, 0},
				{12, 2, 1},
				{13, 3, 2},
				{14, nil, 3},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("with one common column", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"},
			[]Type{Int64, Int64, Int64}, [][]interface{}{
				{2, 20, 22},
				{5, 4, nil},
				{10, -5, 2},
				{14, nil, 0},
				{18, 12, -3},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces([]string{"a", "d", "e", "f"},
			[]Type{Int64, Int64, Int64, Int64}, [][]interface{}{
				{10, nil, 30, 5},
				{14, 40, 10, 13},
				{18, 41, 0, nil},
				{42, nil, 4, 42},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e", "f"},
			[]Type{Int64, Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
				{2, 20, 22, nil, nil, nil},
				{5, 4, nil, nil, nil, nil},
				{10, -5, 2, nil, 30, 5},
				{14, nil, 0, 40, 10, 13},
				{18, 12, -3, 41, 0, nil},
				{42, nil, nil, nil, 4, 42},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("with two common columns", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col2"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{2, 0.0, 2},
				{3, 3.0, 3},
				{5, 4.4, 4},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1", "col2"},
			[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
				{1, 1.1, 1, 1},
				{1, 1.1, nil, 1},
				{2, nil, 3, nil},
				{3, 3.3, 4, nil},
				{4, 4.4, 5, nil},
				{2, 0.0, nil, 2},
				{3, 3.0, nil, 3},
				{5, 4.4, nil, 4},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	// Advanced OuterJoin test with the following features:
	// - 2 common columns at different indexes
	// - 4 common rows at different indexes, including full nil rows
	t.Run("advanced", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces([]string{"index1", "col1", "col2", "index2", "col3"},
			[]Type{Int64, Int64, Int64, Float64, Int64}, [][]interface{}{
				{2, 20, 22, 0.0, 3},
				{nil, nil, nil, nil, nil},
				{5, 4, nil, 2.0, 5},
				{10, -5, 2, nil, 7},
				{2, nil, 0, 0.0, 11},
				{14, nil, 0, 6.0, 0},
				{0, 0, 0, 0.0, 0},
				{18, 4, -3, 2.0, -1},
				{nil, nil, nil, nil, nil},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces([]string{"col4", "index1", "index2", "col5"},
			[]Type{Int64, Int64, Float64, Int64}, [][]interface{}{
				{10, -5, 30.0, nil},
				{10, -3, 1.0, nil},
				{14, 40, 10.0, 13},
				{nil, 41, 0.0, nil},
				{42, -5, 30.0, 6},
				{41, 5, 2.0, 6},
				{nil, nil, nil, nil},
				{40, -5, 30.0, 6},
				{0, 0, 0.0, 0},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces([]string{"index1", "col1", "col2", "index2", "col3", "col4", "col5"},
			[]Type{Int64, Int64, Int64, Float64, Int64, Int64, Int64}, [][]interface{}{
				{2, 20, 22, 0.0, 3, nil, nil},
				{nil, nil, nil, nil, nil, nil, nil},
				{5, 4, nil, 2.0, 5, 41, 6},
				{10, -5, 2, nil, 7, nil, nil},
				{2, nil, 0, 0.0, 11, nil, nil},
				{14, nil, 0, 6.0, 0, nil, nil},
				{0, 0, 0, 0.0, 0, 0, 0},
				{18, 4, -3, 2.0, -1, nil, nil},
				{nil, nil, nil, nil, nil, nil, nil},
				{-5, nil, nil, 30.0, nil, 10, nil},
				{-3, nil, nil, 1.0, nil, 10, nil},
				{40, nil, nil, 10.0, nil, 14, 13},
				{41, nil, nil, 0.0, nil, nil, nil},
				{-5, nil, nil, 30.0, nil, 42, 6},
				{-5, nil, nil, 30.0, nil, 40, 6},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("no common rows", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "col2"},
			[]Type{Int64, Int64}, [][]interface{}{
				{10, 10},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1", "col2"},
			[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
				{1, 1.1, 1, nil},
				{1, 1.1, nil, nil},
				{2, nil, 3, nil},
				{3, 3.3, 4, nil},
				{4, 4.4, 5, nil},
				{10, nil, nil, 10},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("incompatible types", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1"},
			[]Type{Float64}, [][]interface{}{
				{1},
			})
		require.NoError(t, err)
		defer func() {
			if r := recover(); r == nil ||
				r.(error).Error() != "bow.Join: left and right bow on join columns are of incompatible types: index1" {
				t.Errorf("indexes of b1 and b2 are incompatible and should panic. Have %v, expect %v",
					r, "bow Join: left and right bow on join columns are of incompatible types: index1")
			}
		}()
		b1.OuterJoin(b2)
	})

	t.Run("no common columns", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index3"},
			[]Type{Float64}, [][]interface{}{
				{1.1},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1", "index3"},
			[]Type{Int64, Float64, Int64, Float64}, [][]interface{}{
				{1, 1.1, 1, nil},
				{1, 1.1, nil, nil},
				{2, nil, 3, nil},
				{3, 3.3, 4, nil},
				{4, 4.4, 5, nil},
				{nil, nil, nil, 1.1},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("with metadata", func(t *testing.T) {
		b1, err := NewBowWithMetadata(NewMetadata([]string{"k1"}, []string{"v1"}),
			NewSeries("index1", []int64{1}, nil),
		)
		require.NoError(t, err)

		b2, err := NewBowWithMetadata(NewMetadata([]string{"k2"}, []string{"v2"}),
			NewSeries("index1", []int64{1}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k1", "k2"}, []string{"v1", "v2"}),
			NewSeries("index1", []int64{1}, nil),
		)
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("with only nils in common rows", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"A", "B"},
			[]Type{Int64, Int64}, [][]interface{}{
				{1, 56},
				{nil, 78},
				{4, 11},
				{5, nil},
				{6, nil},
				{7, 15},
				{9, 25},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"A", "C"},
			[]Type{Int64, Int64}, [][]interface{}{
				{1, 12},
				{nil, nil},
				{4, 21},
				{nil, 69},
				{6, 19},
				{nil, 71},
				{nil, 18},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"A", "B", "C"},
			[]Type{Int64, Int64, Int64}, [][]interface{}{
				{1, 56, 12},
				{nil, 78, nil},
				{nil, 78, 69},
				{nil, 78, 71},
				{nil, 78, 18},
				{4, 11, 21},
				{5, nil, nil},
				{6, nil, 19},
				{7, 15, nil},
				{9, 25, nil},
			})
		require.NoError(t, err)

		result := b1.OuterJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})
}

func TestBow_InnerJoin(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col2"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{2, 0.0, 2},
				{3, 3.0, 3},
				{5, 4.4, 4},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1", "col2"},
			[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
				{1, 1.1, 1, 1},
				{1, 1.1, nil, 1},
			})
		require.NoError(t, err)

		result := b1.InnerJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("time series like", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces([]string{"a", "b"},
			[]Type{Int64, Int64}, [][]interface{}{
				{10, 0},
				{11, 1},
				{12, 2},
				{13, 3},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces([]string{"a", "c"},
			[]Type{Int64, Int64}, [][]interface{}{
				{11, 0},
				{12, 1},
				{13, 2},
				{14, 3},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"},
			[]Type{Int64, Int64, Int64}, [][]interface{}{
				{11, 1, 0},
				{12, 2, 1},
				{13, 3, 2},
			})
		require.NoError(t, err)

		result := b1.InnerJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("no common rows", func(t *testing.T) {
		b1, err := NewBow(
			NewSeries("index1", []int64{1, 1, 2, 3, 4}, nil),
			NewSeries("index2", []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
			NewSeries("col1", []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
		)
		require.NoError(t, err)

		b2, err := NewBow(
			NewSeries("index1", []int64{10}, nil),
			NewSeries("col2", []int64{10}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBow(
			NewSeries("index1", []int64{}, nil),
			NewSeries("index2", []float64{}, nil),
			NewSeries("col1", []int64{}, []bool{}),
			NewSeries("col2", []int64{}, nil),
		)
		require.NoError(t, err)

		result := b1.InnerJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("incompatible types", func(t *testing.T) {
		b1, err := NewBow(
			NewSeries("index1", []int64{1, 1, 2, 3, 4}, nil),
			NewSeries("index2", []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
			NewSeries("col1", []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
		)
		require.NoError(t, err)

		b2, err := NewBow(
			NewSeries("index1", []float64{1}, nil),
		)
		require.NoError(t, err)

		defer func() {
			if r := recover(); r == nil ||
				r.(error).Error() != "bow.Join: left and right bow on join columns are of incompatible types: index1" {
				t.Errorf("indexes of b1 and b2 are incompatible and should panic. Have %v, expect %v",
					r, "bow Join: left and right bow on join columns are of incompatible types: index1")
			}
		}()
		b1.InnerJoin(b2)
	})

	t.Run("no common columns", func(t *testing.T) {
		b1, err := NewBow(
			NewSeries("index1", []int64{1, 1, 2, 3, 4}, nil),
			NewSeries("index2", []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
			NewSeries("col1", []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
		)
		require.NoError(t, err)

		b2, err := NewBow(
			NewSeries("index3", []float64{1.1}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBow(
			NewSeries("index1", []int64{}, nil),
			NewSeries("index2", []float64{}, nil),
			NewSeries("col1", []int64{}, nil),
			NewSeries("index3", []float64{}, []bool{}),
		)
		require.NoError(t, err)

		result := b1.InnerJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("with metadata", func(t *testing.T) {
		b1, err := NewBowWithMetadata(NewMetadata([]string{"k1"}, []string{"v1"}),
			NewSeries("index1", []int64{1}, nil),
		)
		require.NoError(t, err)

		b2, err := NewBowWithMetadata(NewMetadata([]string{"k2"}, []string{"v2"}),
			NewSeries("index1", []int64{1}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k1", "k2"}, []string{"v1", "v2"}),
			NewSeries("index1", []int64{1}, nil),
		)
		require.NoError(t, err)

		result := b1.InnerJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})

	t.Run("with only nils in common rows", func(t *testing.T) {
		b1, err := NewBowFromRowBasedInterfaces(
			[]string{"A", "B"},
			[]Type{Int64, Int64}, [][]interface{}{
				{1, 56},
				{nil, 78},
				{4, 11},
				{5, nil},
				{6, nil},
				{7, 15},
				{9, 25},
			})
		require.NoError(t, err)

		b2, err := NewBowFromRowBasedInterfaces(
			[]string{"A", "C"},
			[]Type{Int64, Int64}, [][]interface{}{
				{1, 12},
				{nil, nil},
				{4, 21},
				{nil, 69},
				{6, 19},
				{nil, 71},
				{nil, 18},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"A", "B", "C"},
			[]Type{Int64, Int64, Int64}, [][]interface{}{
				{1, 56, 12},
				{nil, 78, nil},
				{nil, 78, 69},
				{nil, 78, 71},
				{nil, 78, 18},
				{4, 11, 21},
				{6, nil, 19},
			})
		require.NoError(t, err)

		result := b1.InnerJoin(b2)
		assert.EqualValues(t, expected.String(), result.String())
	})
}

func BenchmarkBow_Join(b *testing.B) {
	for rows := 10; rows <= 1000; rows *= 10 {
		b.Run(fmt.Sprintf("Inner_%dx%d", rows, 2), func(b *testing.B) {
			b.StopTimer()
			leftBow, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(2),
				OptionGenType(GenTypeIncrementalRandom),
				OptionGenColNames([]string{"A", "B"}))
			require.NoError(b, err)

			rightBow, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(2),
				OptionGenType(GenTypeIncrementalRandom),
				OptionGenColNames([]string{"A", "C"}))
			require.NoError(b, err)

			b.StartTimer()
			for n := 0; n < b.N; n++ {
				leftBow.InnerJoin(rightBow)
			}
		})
		b.Run(fmt.Sprintf("Outer_%dx%d", rows, 2), func(b *testing.B) {
			b.StopTimer()
			leftBow, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(2),
				OptionGenType(GenTypeIncrementalRandom),
				OptionGenColNames([]string{"A", "B"}))
			require.NoError(b, err)

			rightBow, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(2),
				OptionGenType(GenTypeIncrementalRandom),
				OptionGenColNames([]string{"A", "C"}))
			require.NoError(b, err)

			b.StartTimer()
			for n := 0; n < b.N; n++ {
				leftBow.OuterJoin(rightBow)
			}
		})
	}
}
