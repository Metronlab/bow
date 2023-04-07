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
					{0, 3, 3, 4, 0},
					{nil, 3, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			b, err = b.FillMean(1)
			require.NoError(t, err)
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
					{0, 3, 3, 4, 0},
					{-1, 3, nil, nil, -4},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			b, err = b.FillMean()
			require.NoError(t, err)
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
					{0, 1, 3, 4, 0},
					{nil, 1, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			b, err = b.FillNext(1)
			require.NoError(t, err)
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
					{0, 1, 3, 4, 0},
					{-2, 1, nil, nil, -8},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			b, err = b.FillNext()
			require.NoError(t, err)
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
					{0, 4, 3, 4, 0},
					{nil, 4, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			b, err = b.FillPrevious(1)
			require.NoError(t, err)
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
					{0, 4, 3, 4, 0},
					{0, 4, 3, 4, 0},
					{-2, 1, 3, 4, -8},
				})
			require.NoError(t, err)

			b, err = b.FillPrevious()
			require.NoError(t, err)
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
					{0, 2, 3, 4, 0},
					{nil, nil, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)
			res, err := b.FillLinear(0, 1)
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
					{0, nil, 3, 4, 0},
					{nil, nil, nil, nil, nil},
					{-2, 1, nil, nil, -8},
				})
			require.NoError(t, err)

			res, err := b.FillLinear(0, 4)
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), res.String())
		})

		t.Run("Linear refCol not sorted", func(t *testing.T) {
			b := newFreshBow(t, Int64)

			_, err := b.FillLinear(4, 1)
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
					{0.0, 2.5, 3.0, 4.0, 0.0},
					{nil, 2.5, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			b, err = b.FillMean(1)
			require.NoError(t, err)
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
					{0.0, 2.5, 3.0, 4.0, 0.0},
					{-1.0, 2.5, nil, nil, -4.0},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			b, err = b.FillMean()
			require.NoError(t, err)
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
					{0.0, 1.0, 3.0, 4.0, 0.0},
					{nil, 1.0, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			b, err = b.FillNext(1)
			require.NoError(t, err)
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
					{0.0, 1.0, 3.0, 4.0, 0.0},
					{-2.0, 1.0, nil, nil, -8.0},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			b, err = b.FillNext()
			require.NoError(t, err)
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
					{0.0, 4.0, 3.0, 4.0, 0.0},
					{nil, 4.0, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			b, err = b.FillPrevious(1)
			require.NoError(t, err)
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
					{0.0, 4.0, 3.0, 4.0, 0.0},
					{0.0, 4.0, 3.0, 4.0, 0.0},
					{-2.0, 1.0, 3.0, 4.0, -8.0},
				})
			require.NoError(t, err)

			b, err = b.FillPrevious()
			require.NoError(t, err)
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
					{0.0, 1.5, 3.0, 4.0, 0.0},
					{nil, nil, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			res, err := b.FillLinear(0, 1)
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
					{0.0, nil, 3.0, 4.0, 0.0},
					{nil, nil, nil, nil, nil},
					{-2.0, 1.0, nil, nil, -8.0},
				})
			require.NoError(t, err)

			res, err := b.FillLinear(0, 4)
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), res.String())
		})

		t.Run("Linear refCol not sorted", func(t *testing.T) {
			b := newFreshBow(t, Float64)

			_, err := b.FillLinear(4, 1)
			require.Error(t, err)
		})
	})

	t.Run("non numeric", func(t *testing.T) {
		t.Run("Previous", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Boolean, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Boolean, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, "sfr"},
					{0, false, "yop"},
					{-1, true, "yop"},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			b, err = b.FillPrevious()
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Next", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Boolean, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			expected, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Boolean, String},
				[][]interface{}{
					{20, false, "dgr"},
					{13, false, "sfr"},
					{10, false, "yop"},
					{0, true, "yop"},
					{-1, true, "ioi"},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			b, err = b.FillNext()
			require.NoError(t, err)
			assert.EqualValues(t, expected.String(), b.String())
		})

		t.Run("Mean", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Boolean, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)
			_, err = b.FillMean()
			assert.Error(t, err)
		})

		t.Run("Linear", func(t *testing.T) {
			b, err := NewBowFromRowBasedInterfaces(
				[]string{"a", "b", "c"},
				[]Type{Int64, Boolean, String},
				[][]interface{}{
					{20, nil, "dgr"},
					{13, false, "sfr"},
					{10, false, nil},
					{0, nil, "yop"},
					{-1, true, nil},
					{-2, false, "ioi"},
				})
			require.NoError(t, err)

			_, err = b.FillLinear(0, 1)
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

		b1, err = b1.FillPrevious()
		require.NoError(t, err)
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

		b2, err = b2.FillNext()
		require.NoError(t, err)
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

		b3, err = b3.FillMean()
		require.NoError(t, err)
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

		res, err := b4.FillLinear(0, 1)
		require.NoError(t, err)
		assert.EqualValues(t, expected.String(), res.String())
	})
}

func BenchmarkBow_Fill(b *testing.B) {
	for rows := 10; rows <= 100000; rows *= 10 {
		data, err := NewBowFromParquet(fmt.Sprintf(
			"%sbow1-%d-rows.parquet", benchmarkBowsDirPath, rows), false)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("Previous_%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err = data.FillPrevious(3)
				require.NoError(b, err)
			}
		})

		b.Run(fmt.Sprintf("Next_%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err = data.FillNext(3)
				require.NoError(b, err)
			}
		})

		b.Run(fmt.Sprintf("Mean_%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err = data.FillMean(3)
				require.NoError(b, err)
			}
		})

		b.Run(fmt.Sprintf("Linear_%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err = data.FillLinear(0, 3)
				require.NoError(b, err)
			}
		})
	}
}
