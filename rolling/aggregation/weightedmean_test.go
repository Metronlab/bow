package aggregation

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
)

func TestWeightedAverageStep(t *testing.T) {
	runTestCases(t, WeightedAverageStep, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries("time", bow.Int64, []int64{}, nil),
					bow.NewSeries("value", bow.Float64, []float64{}, nil),
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
		{
			name:      "float only nil",
			testedBow: nilBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, nil},
						{20, nil},
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
						{50, 0.1},
						{60, 0.8},
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
						{40, 9.},
						{50, 19.},
						{60, 2.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}

func TestWeightedAverageLinear(t *testing.T) {
	runTestCases(t, WeightedAverageLinear, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries("time", bow.Int64, []int64{}, nil),
					bow.NewSeries("value", bow.Float64, []float64{}, nil),
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
		{
			name:      "sparse bool",
			testedBow: sparseBoolBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, nil},
						{20, nil},
						{30, nil},
						{40, 0.45},
						{50, 0.05},
						{60, 0.4},
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
						{10, nil},
						{20, nil},
						{30, nil},
						{40, 9.},
						{50, 1.5},
						{60, nil},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
