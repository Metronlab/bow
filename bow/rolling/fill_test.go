package rolling

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Fill(t *testing.T) {
	interval := 2.
	b, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Float64, bow.Int64}, [][]interface{}{
		{10., 13.},
		{100, 130},
	})

	timeInterp := NewColumnInterpolation(timeCol, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			return neededPos, nil
		})
	valueInterp := NewColumnInterpolation(valueCol, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			return 999, nil
		})
	interpFloatBool := NewColumnInterpolation(valueCol, []bow.Type{bow.Float64, bow.Bool},
		func(colIndex int, neededPos float64, w bow.Window, full bow.Bow) (interface{}, error) {
			return true, nil
		})

	t.Run("invalid input type", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{})
		_, err := r.
			Fill(timeInterp, interpFloatBool).
			Bow()
		assert.EqualError(t, err, "fill: invalid input type Int64, must be one of [Float64 Bool]")
	})

	t.Run("no options", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{})

		filled, err := r.
			Fill(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Float64, bow.Int64}, [][]interface{}{
			{10., 12., 13.},
			{100, 999, 130},
		})
		assert.Equal(t, true, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{Offset: 1})

		filled, err := r.
			Fill(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Float64, bow.Int64}, [][]interface{}{
			{9., 10., 11., 13.},
			{999, 100, 999, 130},
		})
		assert.Equal(t, true, filled.Equal(expected))
	})
}
