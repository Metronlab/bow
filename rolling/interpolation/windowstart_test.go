package interpolation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowStart(t *testing.T) {
	var interval int64 = 2

	b, err := bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol},
		[]bow.Type{bow.Int64},
		[][]interface{}{
			{10},
			{13},
		})
	require.NoError(t, err)

	t.Run("no options", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Int64},
			[][]interface{}{
				{10},
				{12},
				{13},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("with offset", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Int64},
			[][]interface{}{
				{9},
				{10},
				{11},
				{13},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{Offset: 1.})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})
}
