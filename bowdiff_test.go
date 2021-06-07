package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	t.Run("all columns all supported types with nils", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "c"},
			[]Type{Int64, Float64, Bool},
			[][]interface{}{
				{1, 1., false},
				{2, 2., false},
				{3, 3., true},
				{4, 4., true},
				{nil, nil, nil},
				{5, 5., false},
			})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "c"},
			[]Type{Int64, Float64, Bool},
			[][]interface{}{
				{nil, nil, nil},
				{1, 1., false},
				{1, 1., true},
				{1, 1., false},
				{nil, nil, nil},
				{nil, nil, nil},
			})
		require.NoError(t, err)

		calc, err := b.Diff()
		assert.NoError(t, err)
		assert.EqualValues(t, expected.String(), calc.String())
	})

	t.Run("one column all supported types", func(t *testing.T) {
		b, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Float64, Bool}, [][]interface{}{
			{1, 1., false},
			{2, 2., false},
			{3, 3., true},
		})
		require.NoError(t, err)

		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Float64, Bool}, [][]interface{}{
			{1, nil, false},
			{2, 1., false},
			{3, 1., true},
		})
		require.NoError(t, err)

		calc, err := b.Diff("b")
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

		calc, err := b.Diff("unknown")
		assert.Error(t, err)
		assert.Nil(t, calc)
	})
}
