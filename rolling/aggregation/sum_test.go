package aggregation

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
)

func TestSum(t *testing.T) {
	runTestCases(t, Sum, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewPrevSeries("time", bow.Int64, []int64{}, nil),
					bow.NewPrevSeries("value", bow.Float64, []float64{}, nil),
				)
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			name:      "sparse float",
			testedBow: sparseFloatBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, 0.},
						{30, 0.},
						{40, 10.},
						{50, 30.},
						{60, 30.},
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
						{20, 0.},
						{30, 0.},
						{40, 0.},
						{50, 1.},
						{60, 1.},
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
						{20, 0.},
						{30, 0.},
						{40, 10.},
						{50, 30.},
						{60, 20.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
