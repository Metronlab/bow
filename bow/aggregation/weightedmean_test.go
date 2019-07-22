package aggregation

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"

	"github.com/stretchr/testify/assert"
)

func TestWeightedAverageStep(t *testing.T) {
	runTestCases(t, WeightedAverageStep, nil, []bowTest{
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
			TestedBow: sparseFloatBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, nil},
						{30, nil},
						{40, 10 * 0.9},
						{50, 10*0.1 + 20*0.9},
						{60, 10*0.8 + 20*0.1},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}

func TestWeightedAverageLinear(t *testing.T) {
	runTestCases(t, WeightedAverageLinear, nil, []bowTest{
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
			TestedBow: sparseFloatBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, nil},
						{20, nil},
						{30, nil},
						{40, 10 * 0.9},
						{50, 15 * 0.1},
						{60, 15 * 0.8},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
