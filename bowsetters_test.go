package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_SetColName(t *testing.T) {
	b, err := NewBowFromRowBasedInterfaces([]string{"time", "val"}, []Type{Int64, Float64}, [][]interface{}{
		{10, 0.1},
		{11, 0.2},
	})
	require.NoError(t, err)
	expected, err := NewBowFromRowBasedInterfaces([]string{"time", "value"}, []Type{Int64, Float64}, [][]interface{}{
		{10, 0.1},
		{11, 0.2},
	})
	require.NoError(t, err)

	t.Run("valid", func(t *testing.T) {
		res, err := b.SetColName(1, "value")
		require.NoError(t, err)
		assert.EqualValues(t, expected.String(), res.String())
	})

	t.Run("invalid colIndex", func(t *testing.T) {
		_, err = b.SetColName(2, "value")
		require.Error(t, err)
	})

	t.Run("invalid newName", func(t *testing.T) {
		_, err = b.SetColName(0, "")
		require.Error(t, err)
	})
}
