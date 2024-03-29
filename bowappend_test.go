package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendBows(t *testing.T) {
	t.Run("no bows", func(t *testing.T) {
		appended, err := AppendBows()
		assert.NoError(t, err)
		assert.Nil(t, appended)
	})

	t.Run("one empty bow", func(t *testing.T) {
		b, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{{}})
		appended, err := AppendBows(b)
		assert.NoError(t, err)
		assert.True(t, appended.Equal(b), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", b, appended))
	})

	t.Run("first empty bow", func(t *testing.T) {
		b1, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{{}})
		b2, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{
				{1},
			})
		appended, err := AppendBows(b1, b2)
		assert.NoError(t, err)
		assert.True(t, appended.Equal(b2), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", b2, appended))
	})

	t.Run("several empty bows", func(t *testing.T) {
		b, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{{}})
		appended, err := AppendBows(b, b)
		assert.NoError(t, err)
		assert.True(t, appended.Equal(b), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", b, appended))
	})

	t.Run("schema mismatch", func(t *testing.T) {
		b1, _ := NewBowFromColBasedInterfaces(
			[]string{"i", "s"},
			[]Type{Int64, String},
			[][]interface{}{
				{"hey"},
				{1},
			})
		b2, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64},
			[][]interface{}{
				{1},
			})

		assert.Panics(t, func() { _, _ = AppendBows(b1, b2) })
	})

	t.Run("type mismatch", func(t *testing.T) {
		b1, _ := NewBowFromColBasedInterfaces(
			[]string{"i", "s"},
			[]Type{Int64, Int64},
			[][]interface{}{
				{1},
				{1},
			})
		b2, _ := NewBowFromColBasedInterfaces(
			[]string{"a"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{1},
				{1.},
			})

		assert.Panics(t, func() { _, _ = AppendBows(b1, b2) })
	})

	t.Run("3 bows of 2 cols", func(t *testing.T) {
		b1, _ := NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{1, 2, 3},
				{.1, .2, .3},
			})
		b2, _ := NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{4, 5},
				{.4, .5},
			})
		b3, _ := NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{6},
				{.6},
			})

		appended, err := AppendBows(b1, b2, b3)
		expected, _ := NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{1, 2, 3, 4, 5, 6},
				{.1, .2, .3, .4, .5, .6},
			})
		assert.NoError(t, err)
		assert.True(t, appended.Equal(expected), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", expected, appended))
	})

	t.Run("2 bows with the same metadata", func(t *testing.T) {
		b1, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 2}, nil),
			NewSeries("value", Float64, []float64{.1, .2}, nil),
		)
		require.NoError(t, err)

		b2, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{3, 4}, nil),
			NewSeries("value", Float64, []float64{.3, .4}, nil),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("time", Int64, []int64{1, 2, 3, 4}, nil),
			NewSeries("value", Float64, []float64{.1, .2, .3, .4}, nil),
		)
		require.NoError(t, err)

		appended, err := AppendBows(b1, b2)
		assert.NoError(t, err)

		assert.Equal(t, expected.String(), appended.String())
	})

	t.Run("same column names but different types", func(t *testing.T) {
		b1, err := NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{1, 2},
				{.1, .2},
			})
		require.NoError(t, err)
		b2, err := NewBowFromColBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Int64},
			[][]interface{}{
				{3},
				{3},
			})
		require.NoError(t, err)

		_, err = AppendBows(b1, b2)
		assert.Error(t, err)
	})
}

func BenchmarkAppendBows(b *testing.B) {
	for rows := 10; rows <= 100000; rows *= 10 {
		b1, err := NewBow(
			NewSeries("time", Int64, make([]int64, rows), nil),
			NewSeries("value", Float64, make([]float64, rows), nil))
		require.NoError(b, err)

		b2, err := NewBow(
			NewSeries("time", Int64, make([]int64, rows), nil),
			NewSeries("value", Float64, make([]float64, rows), nil))
		require.NoError(b, err)

		b.Run(fmt.Sprintf("%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := AppendBows(b1, b2)
				require.NoError(b, err)
			}
		})
	}
}
