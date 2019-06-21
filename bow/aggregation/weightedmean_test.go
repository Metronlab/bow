package aggregation

import (
	"fmt"
	"testing"

	"git.metronlab.com/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestWeightedMean(t *testing.T) {
	r, _ := sparseBow.IntervalRolling(timeCol, 10, bow.RollingOptions{})
	aggregated, err := r.
		Aggregate(
			WindowStart(timeCol),
			WeightedMean(valueCol)).
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
