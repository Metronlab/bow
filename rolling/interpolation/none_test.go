package interpolation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNone(t *testing.T) {
	var interval int64 = 2

	b, err := bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, 1.0},
			{13, 1.3},
		})
	require.NoError(t, err)

	t.Run("no options", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 1.0},
				{12, nil},
				{13, 1.3},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), None(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("with offset", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{9, nil},
				{10, 1.0},
				{11, nil},
				{13, 1.3},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{Offset: 1})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), None(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})
}
