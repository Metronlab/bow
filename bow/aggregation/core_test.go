package aggregation

import (
	"fmt"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	tc = "time"
	vc = "value"

	empty, _ = bow.NewBow(
		bow.NewSeries(tc, bow.Int64, []int64{}, nil),
		bow.NewSeries(vc, bow.Float64, []float64{}, nil),
	)
	sparseFloatBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{tc, vc},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, 10.}, // partially valid window
			{11, nil},
			{20, nil}, // only invalid window

			// empty window

			{40, nil}, // partially valid with start of window invalid
			{41, 10.},
			{50, 10.}, // valid with two values on start of window
			{51, 20.},
			{61, 10.}, // valid with two values NOT on start of window
			{69, 20.},
		})
)

type bowTest struct {
	Name        string
	TestedBow   bow.Bow
	ExpectedBow bow.Bow
}

func runTestCases(t *testing.T, aggregationFunc func(col string) rolling.ColumnAggregation, testCases []bowTest) {
	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			r, err := rolling.IntervalRolling(test.TestedBow, tc, 10, rolling.Options{})
			assert.NoError(t, err)
			aggregated, err := r.
				Aggregate(
					WindowStart(tc),
					aggregationFunc(vc)).
				Bow()
			assert.NoError(t, err)
			assert.NotNil(t, aggregated)

			assert.Equal(t, true, aggregated.Equal(test.ExpectedBow),
				fmt.Sprintf("expect:\n%v\nhave:\n%v", test.ExpectedBow, aggregated))
		})
	}
}
