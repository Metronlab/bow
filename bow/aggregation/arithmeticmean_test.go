package aggregation

import (
	"fmt"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"

	"github.com/stretchr/testify/assert"
)

var (
	timeCol   = "time"
	valueCol  = "value"
	sparseBow = newInputTestBow([][]interface{}{
		{
			10.,
			30., 31.,
		},
		{
			100,
			100, 200,
		},
	})
)

func TestArithmeticMean(t *testing.T) {
	r, _ := rolling.IntervalRolling(sparseBow, timeCol, 10, rolling.Options{})
	aggregated, err := r.
		Aggregate(
			WindowStart(timeCol),
			ArithmeticMean(valueCol)).
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
			100.,
			nil,
			150.,
		},
	})
	assert.Equal(t, true, aggregated.Equal(expected),
		fmt.Sprintf("expect:\n%v\nhave:\n%v", expected, aggregated))
}

func newInputTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Float64, bow.Int64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}

func newOutputTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Float64, bow.Float64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
