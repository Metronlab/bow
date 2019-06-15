package aggregation

import (
	"testing"

	"git.metronlab.com/backend_libraries/go-bow/bow"

	"github.com/stretchr/testify/assert"
)

var (
	timeCol   = "time"
	valueCol  = "value"
	sparseBow = newIntervalRollingTestBow([][]interface{}{
		{
			10,
			30, 31,
		},
		{
			100.0,
			100.0, 200.0,
		},
	})
)

func TestArithmeticMean(t *testing.T) {
	r, _ := sparseBow.IntervalRolling(timeCol, 10, bow.RollingOptions{})
	aggregated, err := r.
		Aggregate(
			WindowStart(timeCol),
			ArithmeticMean(valueCol)).
		Bow()
	assert.Nil(t, err)
	assert.NotNil(t, aggregated)
	expected := newIntervalRollingTestBow([][]interface{}{
		{
			10,
			20,
			30,
		},
		{
			100.0,
			nil,
			150.0,
		},
	})
	assert.Equal(t, true, aggregated.Equal(expected))
}

func newIntervalRollingTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Int64, bow.Float64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
