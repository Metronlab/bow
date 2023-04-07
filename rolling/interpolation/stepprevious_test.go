package interpolation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	timeCol  = "time"
	valueCol = "value"
)

func TestStepPrevious(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 1.0},
				{13, 1.3},
			})
		require.NoError(t, err)

		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 1.0},
				{12, 1.0},
				{13, 1.3},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, 2, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), StepPrevious(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("bool", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Boolean},
			[][]interface{}{
				{10, true},
				{13, false},
			})
		require.NoError(t, err)

		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Boolean},
			[][]interface{}{
				{10, true},
				{12, true},
				{13, false},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, 2, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), StepPrevious(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("string", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.String},
			[][]interface{}{
				{10, "test"},
				{13, "test2"},
			})
		require.NoError(t, err)

		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.String},
			[][]interface{}{
				{10, "test"},
				{12, "test"},
				{13, "test2"},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, 2, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), StepPrevious(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("with offset", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 1.0},
				{13, 1.3},
			})
		require.NoError(t, err)

		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{9, nil},
				{10, 1.0},
				{11, 1.0},
				{13, 1.3},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, 2, rolling.Options{Offset: 1})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), StepPrevious(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("with nils", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 1.0},
				{11, nil},
				{13, nil},
				{15, 1.5},
			})
		require.NoError(t, err)

		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 1.0},
				{11, nil},
				{12, 1.0},
				{13, nil},
				{14, 1.0},
				{15, 1.5},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, 2, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), StepPrevious(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})
}
