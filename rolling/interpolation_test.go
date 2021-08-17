package rolling

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Fill(t *testing.T) {
	timeInterp := NewColumnInterpolation(timeCol, []bow.Type{bow.Int64},
		func(colIndex int, w Window, full, prevRow bow.Bow) (interface{}, error) {
			return w.Start, nil
		})
	valueInterp := NewColumnInterpolation(valueCol, []bow.Type{bow.Int64, bow.Float64},
		func(colIndex int, w Window, full, prevRow bow.Bow) (interface{}, error) {
			return 9.9, nil
		})

	t.Run("invalid input type", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 13},
			{1.0, 1.3},
		})
		r, _ := IntervalRolling(b, timeCol, 2, Options{})
		interp := NewColumnInterpolation(valueCol, []bow.Type{bow.Int64, bow.Bool},
			func(colIndex int, w Window, full, prevRow bow.Bow) (interface{}, error) {
				return true, nil
			})
		_, err := r.
			Interpolate(timeInterp, interp).
			Bow()
		assert.EqualError(t, err, "fill: interpolation accepts types [int64 bool], got type float64")
	})

	t.Run("missing interval column", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 13},
			{1.0, 1.3},
		})
		r, _ := IntervalRolling(b, timeCol, 2, Options{})
		_, err := r.
			Interpolate(valueInterp).
			Bow()
		assert.EqualError(t, err, fmt.Sprintf("fill: must keep interval column '%s'", timeCol))
	})

	t.Run("empty bow", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{},
			{},
		})
		r, _ := IntervalRolling(b, timeCol, 2, Options{})

		filled, err := r.
			Interpolate(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		assert.True(t, filled.Equal(b), fmt.Sprintf("expected %v\nactual  %v", b, filled))
	})

	t.Run("no options", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 13},
			{1.0, 1.3},
		})
		r, _ := IntervalRolling(b, timeCol, 2, Options{})

		filled, err := r.
			Interpolate(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 12, 13},
			{1.0, 9.9, 1.3},
		})
		assert.True(t, filled.Equal(expected), fmt.Sprintf("expected %v\nactual  %v", expected, filled))
	})

	t.Run("with offset", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 13},
			{1.0, 1.3},
		})
		r, _ := IntervalRolling(b, timeCol, 2, Options{Offset: 1})

		filled, err := r.
			Interpolate(timeInterp, valueInterp).
			Bow()
		assert.Nil(t, err)

		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{9, 10, 11, 13},
			{9.9, 1.0, 9.9, 1.3},
		})
		assert.True(t, filled.Equal(expected), fmt.Sprintf("expected %v\nactual  %v", expected, filled))
	})
}
