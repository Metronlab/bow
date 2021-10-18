package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_SetColName(t *testing.T) {
	b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("oldName", []float64{0.1, 0.2}, nil),
	)
	require.NoError(t, err)

	expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("newName", []float64{0.1, 0.2}, nil),
	)
	require.NoError(t, err)

	t.Run("valid", func(t *testing.T) {
		res, err := b.RenameCol(0, "newName")
		require.NoError(t, err)
		assert.EqualValues(t, expected.String(), res.String())
	})

	t.Run("invalid colIndex", func(t *testing.T) {
		_, err = b.RenameCol(1, "newName")
		require.Error(t, err)
	})

	t.Run("invalid newName", func(t *testing.T) {
		_, err = b.RenameCol(0, "")
		require.Error(t, err)
	})
}

func TestBow_Apply(t *testing.T) {
	b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("unchanged", []float64{0.1, 0.2}, nil),
		NewSeries("apply", []float64{0.1, 0.2}, nil),
	)
	require.NoError(t, err)

	expect, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("unchanged", []float64{0.1, 0.2}, nil),
		NewSeries("apply", []string{"0.100000", "0.200000"}, nil),
	)
	require.NoError(t, err)

	res, err := b.Apply(1, String, String.Convert)
	require.NoError(t, err)
	ExpectEqual(t, expect, res)
}

func TestBow_Filter(t *testing.T) {
	b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewSeries("string", []string{"0.1", "0.2"}, nil),
		NewSeries("float", []float64{0.1, 0.2}, nil),
	)
	require.NoError(t, err)

	t.Run("empty filter", func(t *testing.T) {
		res := b.Filter()
		ExpectEqual(t, b, res)
	})

	t.Run("empty result", func(t *testing.T) {
		res := b.Filter(b.MakeFilterValues(0, "not found"))
		ExpectEqual(t, b.NewEmptySlice(), res)
	})

	t.Run("match one comparator", func(t *testing.T) {
		res := b.Filter(b.MakeFilterValues(0, "0.1"))
		ExpectEqual(t, b.NewSlice(0, 1), res)
	})

	t.Run("match two", func(t *testing.T) {
		res := b.Filter(
			b.MakeFilterValues(0, "0.1"),
			b.MakeFilterValues(1, 0.1),
		)
		ExpectEqual(t, b.NewSlice(0, 1), res)
	})

	t.Run("match half", func(t *testing.T) {
		res := b.Filter(
			b.MakeFilterValues(0, "0.1"),
			b.MakeFilterValues(1, 0.2),
		)
		ExpectEqual(t, b.NewEmptySlice(), res)
	})

	t.Run("match all", func(t *testing.T) {
		res := b.Filter(
			b.MakeFilterValues(0, "0.1", "0.2"),
			b.MakeFilterValues(1, 0.1, 0.2),
		)
		ExpectEqual(t, b, res)
	})

	t.Run("match all lazy", func(t *testing.T) {
		res := b.Filter(
			b.MakeFilterValues(0, "0.1", "0.2"),
			b.MakeFilterValues(1, "0.1", "0.2"),
		)
		ExpectEqual(t, b, res)
	})

	t.Run("not convertible", func(t *testing.T) {
		res := b.Filter(
			b.MakeFilterValues(1, "not a number"),
		)
		ExpectEqual(t, b.NewEmptySlice(), res)
	})

	t.Run("match non concomitant", func(t *testing.T) {
		b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("string", []string{"0.1", "0.2", "0.3"}, nil),
			NewSeries("float", []float64{0.1, 0.2, 0.3}, nil),
		)
		require.NoError(t, err)
		expect, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
			NewSeries("string", []string{"0.1", "0.3"}, nil),
			NewSeries("float", []float64{0.1, 0.3}, nil),
		)
		require.NoError(t, err)

		res := b.Filter(
			b.MakeFilterValues(0, "0.1", "0.3"),
		)
		ExpectEqual(t, expect, res)
	})
}
