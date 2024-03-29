package aggregation

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling/transformation"
	"github.com/stretchr/testify/assert"
)

func TestIntegralStep(t *testing.T) {
	runTestCases(t, IntegralStep, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries(timeCol, bow.Int64, []int64{}, nil),
					bow.NewSeries(valueCol, bow.Float64, []float64{}, nil),
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
					[]string{timeCol, valueCol},
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
		{
			name:      "sparse bool",
			testedBow: sparseBoolBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{timeCol, valueCol},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, nil},
						{30, nil},
						{40, 0.},
						{50, 1.},
						{60, 8.},
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
					[]string{timeCol, valueCol},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 100.},
						{20, nil},
						{30, nil},
						{40, 100 * 0.9},
						{50, 100*0.1 + 200*0.9},
						{60, 20.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}

func TestIntegralStep_scaled(t *testing.T) {
	factor := 0.1
	transforms := []transformation.Func{
		func(x interface{}) (interface{}, error) {
			if x == nil {
				return nil, nil
			}
			return x.(float64) * factor, nil
		},
	}
	runTestCases(t, IntegralStep, transforms, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries(timeCol, bow.Int64, []int64{}, nil),
					bow.NewSeries(valueCol, bow.Float64, []float64{}, nil),
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
					[]string{timeCol, valueCol},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, factor * (100.)},
						{20, nil},
						{30, nil},
						{40, factor * (100 * 0.9)},
						{50, factor * (100*0.1 + 200*0.9)},
						{60, factor * (100*0.8 + 200*0.1)},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}

func TestIntegralTrapezoid(t *testing.T) {
	runTestCases(t, IntegralTrapezoid, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries(timeCol, bow.Int64, []int64{}, nil),
					bow.NewSeries(valueCol, bow.Float64, []float64{}, nil),
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
					[]string{timeCol, valueCol},
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
		{
			name:      "sparse bool",
			testedBow: sparseBoolBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{timeCol, valueCol},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, nil},
						{20, nil},
						{30, nil},
						{40, 4.5},
						{50, 0.5},
						{60, 4.},
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
					[]string{timeCol, valueCol},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, nil},
						{20, nil},
						{30, nil},
						{40, 9 * 10.},
						{50, 15.},
						{60, nil},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
