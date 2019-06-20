package bow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Fill(t *testing.T) {
	rollInterval := 10.
	fillInterval := 2.
	r, _ := sparseBow.IntervalRolling(timeCol, rollInterval, RollingOptions{})
	timeInterp := NewColumnInterpolation(timeCol, []Type{Int64, Float64},
		func(colIndex int, neededPos float64, w Window) (interface{}, error) {
			return neededPos, nil
		})
	valueInterp := NewColumnInterpolation(valueCol, []Type{Int64, Float64},
		func(colIndex int, neededPos float64, w Window) (interface{}, error) {
			return int64(99), nil
		})

	filled, err := r.
		Fill(fillInterval, timeInterp, valueInterp).
		Bow()
	assert.Nil(t, err)

	expected, _ := NewBowFromColumnBasedInterfaces(
		[]string{"time", "value"},
		[]Type{Float64, Int64},
		[][]interface{}{
			{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
			{10, 99, 99, 15, 16, 99, 99, 99, 99, 25, 99, 99, 29},
		})
	assert.Equal(t, true, filled.Equal(expected))
}
