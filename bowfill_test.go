package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFreshBow(t *testing.T, typ Type) Bow {
	b, err := NewBowFromRowBasedInterfaces(
		[]string{"a", "b", "c", "d", "e"},
		[]Type{typ, typ, typ, typ, typ},
		[][]interface{}{
			{20, 6, 30, 400, -10},
			{13, nil, nil, nil, nil},
			{10, 4, 10, 10, -5},
			{1, nil, nil, 120, nil},
			{0, nil, 3, 4, 0},
			{nil, nil, nil, nil, nil},
			{-2, 1, nil, nil, -8},
		})
	require.NoError(t, err)

	return b
}

func TestFill(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		t.Run("Mean one column", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 5, nil, nil, nil},
					{10, 4, 10, 10, -5},
					{1, 3, nil, 120, nil},
					{0, 3, 3, 4, 0},
					{nil, 3, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillMean("b"))
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Mean all columns", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 5, 20, 205, -8},
					{10, 4, 10, 10, -5},
					{1, 3, 7, 120, -3},
					{0, 3, 3, 4, 0},
					{-1, 3, nil, nil, -4},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillMean())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Next one column", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 4, nil, nil, nil},
					{10, 4, 10, 10, -5},
					{1, 1, nil, 120, nil},
					{0, 1, 3, 4, 0},
					{nil, 1, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillNext("b"))
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Next all columns", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 4, 10, 10, -5},
					{10, 4, 10, 10, -5},
					{1, 1, 3, 120, 0},
					{0, 1, 3, 4, 0},
					{-2, 1, nil, nil, -8},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillNext())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Previous one column", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 6, nil, nil, nil},
					{10, 4, 10, 10, -5},
					{1, 4, nil, 120, nil},
					{0, 4, 3, 4, 0},
					{nil, 4, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillPrevious("b"))
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Previous all columns", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 6, 30, 400, -10},
					{10, 4, 10, 10, -5},
					{1, 4, 10, 120, -5},
					{0, 4, 3, 4, 0},
					{0, 4, 3, 4, 0},
					{-2, 1, 3, 4, -8},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillPrevious())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Linear refCol a toFillCol b (desc)", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, 5, nil, nil, nil},
					{10, 4, 10, 10, -5},
					{1, 2, nil, 120, nil},
					{0, 2, 3, 4, 0},
					{nil, nil, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)
			res, err := b.FillLinear("a", "b")
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), res.String())
		})

		t.Run("Linear refCol a toFillCol e (asc)", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Int64, Int64, Int64, Int64, Int64},
				[][]interface{}{
					{20, 6, 30, 400, -10},
					{13, nil, nil, nil, -7},
					{10, 4, 10, 10, -5},
					{1, nil, nil, 120, -1},
					{0, nil, 3, 4, 0},
					{nil, nil, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			res, err := b.FillLinear("a", "e")
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), res.String())
		})

		t.Run("Linear refCol not sorted", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			_, err := b.FillLinear("d", "b")
			require.Error(t, err)
		})
	})

	t.Run("float64", func(t *testing.T) {
		t.Run("Mean one column", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 5.0, nil, nil, nil},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 2.5, nil, 120.0, nil},
					{0.0, 2.5, 3.0, 4.0, 0.0},
					{nil, 2.5, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillMean("b"))
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Mean all columns", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 5.0, 20.0, 205.0, -7.5},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 2.5, 6.5, 120.0, -2.5},
					{0.0, 2.5, 3.0, 4.0, 0.0},
					{-1.0, 2.5, nil, nil, -4.0},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillMean())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Next one column", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 4.0, nil, nil, nil},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 1.0, nil, 120.0, nil},
					{0.0, 1.0, 3.0, 4.0, 0.0},
					{nil, 1.0, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillNext("b"))
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Next all columns", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 4.0, 10.0, 10.0, -5.0},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 1.0, 3.0, 120.0, 0.0},
					{0.0, 1.0, 3.0, 4.0, 0.0},
					{-2.0, 1.0, nil, nil, -8.0},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillNext())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Previous one column", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 6.0, nil, nil, nil},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 4.0, nil, 120.0, nil},
					{0.0, 4.0, 3.0, 4.0, 0.0},
					{nil, 4.0, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillPrevious("b"))
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Previous all columns", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 6.0, 30.0, 400.0, -10.0},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 4.0, 10.0, 120.0, -5.0},
					{0.0, 4.0, 3.0, 4.0, 0.0},
					{0.0, 4.0, 3.0, 4.0, 0.0},
					{-2.0, 1.0, 3.0, 4.0, -8.0},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillPrevious())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Linear refCol a toFillCol b (desc)", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, 4.6, nil, nil, nil},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, 1.75, nil, 120.0, nil},
					{0.0, 1.5, 3.0, 4.0, 0.0},
					{nil, nil, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			res, err := b.FillLinear("a", "b")
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), res.String())
		})

		t.Run("Linear refCol a toFillCol e (asc)", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c", "d", "e"},
				[]Type{Float64, Float64, Float64, Float64, Float64},
				[][]interface{}{
					{20.0, 6.0, 30.0, 400.0, -10.0},
					{13.0, nil, nil, nil, -6.5},
					{10.0, 4.0, 10.0, 10.0, -5.0},
					{1.0, nil, nil, 120.0, -0.5},
					{0.0, nil, 3.0, 4.0, 0.0},
					{nil, nil, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			res, err := b.FillLinear("a", "e")
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), res.String())
		})

		t.Run("Linear refCol not sorted", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			_, err := b.FillLinear("d", "b")
			require.Error(t, err)
		})
	})

	t.Run("non numeric", func(t *testing.T) {
		t.Run("Previous", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Bool, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{1, true, "hey"},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Bool, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, "sfr"},
					{1, true, "hey"},
					{0, true, "yop"},
					{-1, true, "yop"},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillPrevious())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Next", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Bool, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{1, true, "hey"},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Bool, String},
				[][]interface{}{
					{20, false, "dgr"},
					{13, false, "sfr"},
					{10, false, "hey"},
					{1, true, "hey"},
					{0, true, "yop"},
					{-1, true, "ioi"},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			require.NoError(t, b.FillNext())
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Mean", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Bool, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{1, true, "hey"},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			assert.Error(t, b.FillMean())
		})

		t.Run("Linear", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Bool, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{1, true, "hey"},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			_, err = b.FillLinear("a", "b")
			assert.Error(t, err)
		})
	})

	t.Run("with metadata", func(t *testing.T) {
		// Previous
		b1, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 0, 3}, []bool{true, false, true}),
			NewSeries("float", Float64, []float64{1., 0., 3.}, []bool{true, false, true}),
		)
		require.NoError(t, err)
		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 1, 3}, []bool{true, true, true}),
			NewSeries("float", Float64, []float64{1., 1., 3.}, []bool{true, true, true}),
		)
		require.NoError(t, err)

		require.NoError(t, b1.FillPrevious())
		assert.Equal(t, expected.String(), b1.String())

		// Next
		b2, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 0, 3}, []bool{true, false, true}),
			NewSeries("float", Float64, []float64{1., 0., 3.}, []bool{true, false, true}),
		)
		require.NoError(t, err)
		expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 3, 3}, []bool{true, true, true}),
			NewSeries("float", Float64, []float64{1., 3., 3.}, []bool{true, true, true}),
		)
		require.NoError(t, err)

		require.NoError(t, b2.FillNext())
		assert.Equal(t, expected.String(), b2.String())

		// Mean
		b3, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 0, 3}, []bool{true, false, true}),
			NewSeries("float", Float64, []float64{1., 0., 3.}, []bool{true, false, true}),
		)
		require.NoError(t, err)
		expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 2, 3}, []bool{true, true, true}),
			NewSeries("float", Float64, []float64{1., 2., 3.}, []bool{true, true, true}),
		)
		require.NoError(t, err)

		require.NoError(t, b3.FillMean())
		assert.Equal(t, expected.String(), b3.String())

		// Linear
		b4, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 0, 3}, []bool{true, false, true}),
			NewSeries("float", Float64, []float64{1., 0., 3.}, []bool{true, false, true}),
		)
		require.NoError(t, err)
		expected, err = NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("int", Int64, []int64{1, 2, 3}, []bool{true, false, true}),
			NewSeries("float", Float64, []float64{1., 2., 3.}, []bool{true, false, true}),
		)
		require.NoError(t, err)

		res, err := b4.FillLinear("int", "float")
		require.NoError(t, err)
		assert.EqualValues(t, expected.String(), res.String())
	})
}

func BenchmarkBow_Fill(b *testing.B) {
	for cols := 2; cols <= 32; cols *= 8 {
		for rows := 10; rows <= 1000000; rows *= 100 {
			b.Run(fmt.Sprintf("%dx%d_%v_Previous", rows, cols, Float64), func(b *testing.B) {
				benchFillPrevious(rows, cols, Float64, b)
			})
			b.Run(fmt.Sprintf("%dx%d_%v_Next", rows, cols, Float64), func(b *testing.B) {
				benchFillNext(rows, cols, Float64, b)
			})
			b.Run(fmt.Sprintf("%dx%d_%v_Mean", rows, cols, Float64), func(b *testing.B) {
				benchFillMean(rows, cols, Float64, b)
			})
			b.Run(fmt.Sprintf("%dx%d_%v_Linear", rows, cols, Float64), func(b *testing.B) {
				benchFillLinear(rows, cols, Float64, b)
			})
		}
	}
}

func benchFillPrevious(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true))
	require.NoError(b, err)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		require.NoError(b, data.FillPrevious())
	}
}

func benchFillNext(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true))
	require.NoError(b, err)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		require.NoError(b, data.FillNext())
	}
}

func benchFillMean(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true))
	require.NoError(b, err)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		require.NoError(b, data.FillMean())
	}
}

func benchFillLinear(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true),
		GenRefCol(0, false))
	require.NoError(b, err)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := data.FillLinear("0", "1")
		require.NoError(b, err)
	}
}
