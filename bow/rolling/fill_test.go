package rolling

import (
	"fmt"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Fill(t *testing.T) {
	var interval int64 = 2
	b, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
		{10, 13},
		{1.0, 1.3},
	})

	timeInterp := NewColumnInterpolation(timeCol, []bow.Type{bow.Int64},
		func(colIndex int, neededPos int64, w bow.Window, full bow.Bow) (interface{}, error) {
			return neededPos, nil
		})
	valueInterp := NewColumnInterpolation(valueCol, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, neededPos int64, w bow.Window, full bow.Bow) (interface{}, error) {
			return 9.9, nil
		})

	t.Run("invalid input type", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{})
		interp := NewColumnInterpolation(valueCol, []bow.Type{bow.Int64, bow.Bool},
			func(colIndex int, neededPos int64, w bow.Window, full bow.Bow) (interface{}, error) {
				return true, nil
			})
		_, err := r.
			Fill(timeInterp, interp).
			Bow()
		assert.EqualError(t, err, "fill: interpolation accepts types [Int64 Bool], got type Float64")
	})

	t.Run("missing interval column", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{})
		_, err := r.
			Fill(valueInterp).
			Bow()
		assert.EqualError(t, err, fmt.Sprintf("fill: must keep interval column '%s'", timeCol))
	})

	t.Run("no options", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{})

		filled, err := r.
			Fill(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 12, 13},
			{1.0, 9.9, 1.3},
		})
		assert.True(t, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := IntervalRolling(b, timeCol, interval, Options{Offset: 1})

		filled, err := r.
			Fill(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{9, 10, 11, 13},
			{9.9, 1.0, 9.9, 1.3},
		})
		assert.True(t, filled.Equal(expected))
	})
}
