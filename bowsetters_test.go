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
	assert.True(t, res.Equal(expect), "expect:\n%s have:\n%s", expect, res)
}
