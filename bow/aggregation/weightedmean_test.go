package aggregation

import (
	"fmt"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
)

type bowTest struct {
	Name        string
	TestedBow   bow.Bow
	ExpectedBow bow.Bow
}

func TestWeightedMean(t *testing.T) {
	testCases := []bowTest{
		{
			Name:      "empty",
			TestedBow: empty,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries("time", bow.Int64, []int64{}, nil),
					bow.NewSeries("value", bow.Float64, []float64{}, nil),
				)
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			Name:      "sparse",
			TestedBow: sparseBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 100.},
						{20, nil},
						{30, 100.*0.1 + 200.*0.9},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			r, err := rolling.IntervalRolling(test.TestedBow, timeCol, 10, rolling.Options{})
			assert.NoError(t, err)
			aggregated, err := r.
				Aggregate(
					WindowStart(timeCol),
					WeightedAverageStep(valueCol)).
				Bow()
			assert.NoError(t, err)
			assert.NotNil(t, aggregated)

			assert.Equal(t, true, aggregated.Equal(test.ExpectedBow),
				fmt.Sprintf("expect:\n%v\nhave:\n%v", test.ExpectedBow, aggregated))
		})
	}
}
