package aggregation

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
)

func TestArithmeticMean(t *testing.T) {
	runTestCases(t, ArithmeticMean, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeriesFromData("time", bow.Int64, []int64{}, nil),
					bow.NewSeriesFromData("value", bow.Float64, []float64{}, nil),
				)
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			name:      "sparse",
			testedBow: sparseFloatBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, nil},
						{30, nil},
						{40, 10.},
						{50, 15.},
						{60, 15.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			name:      "sparse bool",
			testedBow: sparseBoolBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 1.},
						{20, nil},
						{30, nil},
						{40, 0.},
						{50, 0.5},
						{60, 0.5},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			name:      "sparse string",
			testedBow: sparseStringBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, nil},
						{30, nil},
						{40, 10.},
						{50, 15.},
						{60, 20.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
