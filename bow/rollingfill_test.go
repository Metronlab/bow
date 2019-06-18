package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Fill(t *testing.T) {
	var rollInterval int64 = 10
	var fillInterval int64 = 2
	r, _ := sparseBow.IntervalRolling(timeCol, rollInterval, RollingOptions{})
	timeInterp := NewColumnInterpolation(timeCol, []Type{Int64},
		func(colIndex int, neededPos int64, w Window) (interface{}, error) {
			return neededPos, nil
		})
	valueInterp := NewColumnInterpolation(valueCol, []Type{Int64},
		func(colIndex int, neededPos int64, w Window) (interface{}, error) {
			return .0, nil
		})

	filled, err := r.
		Fill(fillInterval, timeInterp, valueInterp).
		Bow()
	assert.Nil(t, err)

	expected, _ := NewBowFromColumnBasedInterfaces(
		[]string{"time", "value"},
		[]Type{Int64, Float64},
		[][]interface{}{
			{10, 12, 14, 15, 16, 18, 20, 22, 24, 25, 26, 28, 29},
			{10.1, .0, .0, 15.1, 16.1, .0, .0, .0, .0, 25.1, .0, .0, 29.1},
		})
	fmt.Println(expected)
	fmt.Println(filled)
	assert.Equal(t, true, filled.Equal(expected))
}
