package rolling

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Fill(t *testing.T) {
	interval := 2.
	timeInterp := NewColumnInterpolation(timeCol, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			return neededPos, nil
		})
	valueInterp := NewColumnInterpolation(valueCol, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			return int64(99), nil
		})
	interpFloatBool := NewColumnInterpolation(valueCol, []bow.Type{bow.Float64, bow.Bool},
		func(colIndex int, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			return true, nil
		})

	t.Run("invalid input type", func(t *testing.T) {
		r, _ := IntervalRolling(sparseBow, timeCol, interval, Options{})

		_, err := r.
			Fill(timeInterp, interpFloatBool).
			Bow()
		assert.EqualError(t, err, "fill: invalid input type Int64, must be one of [Float64 Bool]")
	})

	t.Run("no options", func(t *testing.T) {
		r, _ := IntervalRolling(sparseBow, timeCol, interval, Options{})

		filled, err := r.
			Fill(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"time", "value"},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
				{10, 99, 99, 15, 16, 99, 99, 99, 99, 25, 99, 99, 29},
			})
		assert.Equal(t, true, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := IntervalRolling(sparseBow, timeCol, interval, Options{Offset: 3})

		filled, err := r.
			Fill(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"time", "value"},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{13., 15., 16., 17., 19., 21., 23., 25., 27., 29.},
				{99, 15, 16, 99, 99, 99, 99, 25, 99, 29},
			})
		assert.Equal(t, true, filled.Equal(expected))
	})
}
