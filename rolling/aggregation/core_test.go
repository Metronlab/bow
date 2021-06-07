package aggregation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/metronlab/bow/transform"
	"github.com/stretchr/testify/assert"
)

var (
	tc = "time"
	vc = "value"

	empty, _ = bow.NewBow(
		bow.NewSeries(tc, bow.Int64, []int64{}, nil),
		bow.NewSeries(vc, bow.Float64, []float64{}, nil),
	)
	nilBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{tc, vc},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, nil},
			{11, nil},
			{20, nil},
		})
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
	sparseBoolBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{tc, vc},
		[]bow.Type{bow.Int64, bow.Bool},
		[][]interface{}{
			{10, true}, // partially valid window
			{11, nil},
			{20, nil}, // only invalid window

			// empty window

			{40, nil}, // partially valid with start of window invalid
			{41, false},
			{50, true}, // valid with two values on start of window
			{51, false},
			{61, true}, // valid with two values NOT on start of window
			{69, false},
		})
	sparseStringBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{tc, vc},
		[]bow.Type{bow.Int64, bow.String},
		[][]interface{}{
			{10, "10."}, // partially valid window
			{11, nil},
			{20, nil}, // only invalid window

			// empty window

			{40, nil}, // partially valid with start of window invalid
			{41, "10."},
			{50, "10."}, // valid with two values on start of window
			{51, "20."},
			{61, "test"}, // valid with two values NOT on start of window
			{69, "20."},
		})
)

type bowTest struct {
	Name        string
	TestedBow   bow.Bow
	ExpectedBow bow.Bow
}

func runTestCases(t *testing.T,
	aggrConstruct rolling.ColumnAggregationConstruct, aggrTransforms []transform.Transform,
	testCases []bowTest) {
	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			r, err := rolling.IntervalRolling(test.TestedBow, tc, 10, rolling.Options{})
			assert.NoError(t, err)
			aggregated, err := r.
				Aggregate(
					WindowStart(tc),
					aggrConstruct(vc).Transform(aggrTransforms...)).
				Bow()
			assert.NoError(t, err)
			assert.NotNil(t, aggregated)

			assert.Equal(t, true, aggregated.Equal(test.ExpectedBow),
				fmt.Sprintf("expect:\n%v\nhave:\n%v", test.ExpectedBow, aggregated))
		})
	}
}
