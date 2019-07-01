package aggregation

import (
	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntegralStep(t *testing.T) {
	runTestCases(t, IntegralStep, []bowTest{
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
						{10, 100.},
						{20, nil},
						{30, nil},
						{40, 100 * 0.9},
						{50, 100*0.1 + 200*0.9},
						{60, 100*0.8 + 200*0.1},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}

func TestIntegralTrapezoid(t *testing.T) {
	runTestCases(t, IntegralTrapezoid, []bowTest{
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
						{40, 9 * 10.},
						{50, 15.},
						{60, 8 * (15.)},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
