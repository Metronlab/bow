package aggregation

import (
	"fmt"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/intepolation"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
)

func TestWeightedMean(t *testing.T) {
	r, _ := rolling.IntervalRolling(sparseBow, timeCol, 10, rolling.Options{})
	aggregated, err := r.
		Aggregate(
			WindowStart(timeCol),
			WeightedMean(valueCol, intepolation.StepPrevious)).
		Bow()
	assert.Nil(t, err)
	assert.NotNil(t, aggregated)
	expected := newOutputTestBow([][]interface{}{
		{
			10.,
			20.,
			30.,
		},
		{
			100.0,
			nil,
			100.*0.1 + 200.*0.9,
		},
	})
	assert.Equal(t, true, aggregated.Equal(expected),
		fmt.Sprintf("expect:\n%v\nhave:\n%v", expected, aggregated))
}
