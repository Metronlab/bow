package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_SetColName(t *testing.T) {
	b, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewPrevSeries("oldName", Float64, []float64{0.1, 0.2}, nil),
	)
	require.NoError(t, err)

	expected, err := NewBowWithMetadata(NewMetadata([]string{"k"}, []string{"v"}),
		NewPrevSeries("newName", Float64, []float64{0.1, 0.2}, nil),
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
