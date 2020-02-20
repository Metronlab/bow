package aggregation

import (
	"testing"

	"github.com/metronlab/bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestDifference(t *testing.T) {
	runTestCases(t, Difference, nil, []bowTest{
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
			Name:      "sparse float64",
			TestedBow: sparseFloatBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 0.},
						{20, nil},
						{30, nil},
						{40, 0.},
						{50, 10.},
						{60, 10.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			Name:      "sparse bool",
			TestedBow: sparseBoolBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 0.},
						{20, nil},
						{30, nil},
						{40, 1.},
						{50, -1.},
						{60, -1.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			Name:      "sparse string",
			TestedBow: sparseStringBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 0.},
						{20, nil},
						{30, nil},
						{40, 0.},
						{50, 10.},
						{60, nil},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
