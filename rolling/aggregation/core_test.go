package aggregation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/metronlab/bow/transform"
	"github.com/stretchr/testify/assert"
)

const (
	timeCol  = "time"
	valueCol = "value"
)

type testCase struct {
	name        string
	testedBow   bow.Bow
	expectedBow bow.Bow
}

var (
	emptyBow, _ = bow.NewBow(
		bow.NewSeries(timeCol, bow.Int64, []int64{}, nil),
		bow.NewSeries(valueCol, bow.Float64, []float64{}, nil),
	)
	nilBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, nil},
			{11, nil},
			{20, nil},
		})
	sparseFloatBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol, valueCol},
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
		[]string{timeCol, valueCol},
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
		[]string{timeCol, valueCol},
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

func runTestCases(t *testing.T, aggrConstruct rolling.ColAggregationConstruct,
	aggrTransforms []transform.Transform, testCases []testCase) {
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			r, err := rolling.IntervalRolling(test.testedBow, timeCol, 10, rolling.Options{})
			assert.NoError(t, err)
			aggregated, err := r.
				Aggregate(
					WindowStart(timeCol),
					aggrConstruct(valueCol).Transform(aggrTransforms...)).
				Bow()
			assert.NoError(t, err)
			assert.NotNil(t, aggregated)

			assert.Equal(t, true, aggregated.Equal(test.expectedBow),
				fmt.Sprintf("expect:\n%v\nhave:\n%v", test.expectedBow, aggregated))
		})
	}
}
