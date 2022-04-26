package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	t.Run("all columns all supported types with nils and metadata", func(t *testing.T) {
		b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("a", Int64,
				[]int64{1, 2, 3, 4, 0, 5},
				[]bool{true, true, true, true, false, true}),
			NewSeries("b", Float64,
				[]float64{1., 2., 3., 4., 0., 5.},
				[]bool{true, true, true, true, false, true}),
			NewSeries("c", Bool,
				[]bool{false, false, true, true, false, false},
				[]bool{true, true, true, true, false, true}),
		)
		require.NoError(t, err)

		expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("a", Int64,
				[]int64{0, 1, 1, 1, 0, 0},
				[]bool{false, true, true, true, false, false}),
			NewSeries("b", Float64,
				[]float64{0., 1., 1., 1., 0., 0.},
				[]bool{false, true, true, true, false, false}),
			NewSeries("c", Bool,
				[]bool{false, false, true, false, false, false},
				[]bool{false, true, true, true, false, false}),
		)
		require.NoError(t, err)

		calc, err := b.Diff()
		assert.NoError(t, err)
		assert.EqualValues(t, expected.String(), calc.String())
	})

	t.Run("one column all supported types", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "c"},
			[]Type{Int64, Float64, Bool},
			[][]interface{}{
				{1, 1., false},
				{2, 2., false},
				{3, 3., true},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "c"},
			[]Type{Int64, Float64, Bool},
			[][]interface{}{
				{1, nil, false},
				{2, 1., false},
				{3, 1., true},
			})
		require.NoError(t, err)
		calc, err := b.Diff(1)
		assert.NoError(t, err)
		assert.EqualValues(t, expected.String(), calc.String())
	})

	t.Run("unsupported type string", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces([]string{"a"}, []Type{String}, [][]interface{}{})
		require.NoError(t, err)

		calc, err := b.Diff()
		assert.Error(t, err)
		assert.Nil(t, calc)
	})

	t.Run("empty", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{})
		require.NoError(t, err)

		calc, err := b.Diff()
		assert.NoError(t, err)
		assert.EqualValues(t, b.String(), calc.String())
	})

	t.Run("missing column", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{})
		require.NoError(t, err)

		calc, err := b.Diff(1)
		assert.Error(t, err)
		assert.Nil(t, calc)
	})
}
