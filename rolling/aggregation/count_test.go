package aggregation

import (
	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCount(t *testing.T) {
	runTestCases(t, Count, nil, []bowTest{
		{
			Name:      "empty",
			TestedBow: empty,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries("time", bow.Int64, []int64{}, nil),
					bow.NewSeries("value", bow.Int64, []float64{}, nil),
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
					[]bow.Type{bow.Int64, bow.Int64},
					[][]interface{}{
						{10, 1},
						{20, 0},
						{30, 0},
						{40, 1},
						{50, 2},
						{60, 2},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
