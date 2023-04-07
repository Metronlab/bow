package interpolation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLinear(t *testing.T) {
	var interval int64 = 2

	ascLinearTestBow, err := bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, 10.},
			{15, 15.},
			{17, 17.},
		})
	require.NoError(t, err)

	t.Run("asc no options", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 10.},
				{12, 12.},
				{14, 14.},
				{15, 15.},
				{16, 16.},
				{17, 17.},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(ascLinearTestBow, timeCol, interval, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), Linear(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("asc with offset", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{9, nil},
				{10, 10.},
				{11, 11.},
				{13, 13.},
				{15, 15.},
				{17, 17.},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(ascLinearTestBow, timeCol, interval, rolling.Options{Offset: 3})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), Linear(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	descLinearTestBow, err := bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, 30.},
			{15, 25.},
			{17, 24.},
		})
	require.NoError(t, err)

	t.Run("desc no options", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 30.},
				{12, 28.},
				{14, 26.},
				{15, 25.},
				{16, 24.5},
				{17, 24.},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(descLinearTestBow, timeCol, interval, rolling.Options{})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), Linear(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("desc with offset", func(t *testing.T) {
		expected, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{9, nil},
				{10, 30.},
				{11, 29.},
				{13, 27.},
				{15, 25.},
				{17, 24.},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(descLinearTestBow, timeCol, interval, rolling.Options{Offset: 3})
		require.NoError(t, err)

		filled, err := r.Interpolate(WindowStart(timeCol), Linear(valueCol)).Bow()
		assert.NoError(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected:\n%s\nactual:\n%s", expected.String(), filled.String()))
	})

	t.Run("string error", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.String},
			[][]interface{}{
				{10, "test"},
				{15, "test2"},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{})
		require.NoError(t, err)

		_, err = r.Interpolate(WindowStart(timeCol), Linear(valueCol)).Bow()
		assert.EqualError(t, err,
			"intervalRolling.validateInterpolation: accepts types [int64 float64], got type utf8")
	})

	t.Run("bool error", func(t *testing.T) {
		b, err := bow.NewBowFromRowBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Boolean},
			[][]interface{}{
				{10, true},
				{15, false},
			})
		require.NoError(t, err)

		r, err := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{})
		require.NoError(t, err)

		res, err := r.Interpolate(WindowStart(timeCol), Linear(valueCol)).Bow()
		assert.EqualError(t, err,
			"intervalRolling.validateInterpolation: accepts types [int64 float64], got type bool",
			"have res: %v", res)
	})
}
