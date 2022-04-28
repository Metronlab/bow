package aggregation

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
)

func TestMode(t *testing.T) {
	var modeFloatBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{timeCol, valueCol},
		[]bow.Type{bow.Int64, bow.Float64},
		[][]interface{}{
			{10, 10.}, // same value window
			{11, 10.},

			{20, 42.}, // most occurrences to 42
			{21, 42.},
			{22, 10.},

			{30, nil}, // most occurrences to 10
			{31, nil},
			{32, 10.},

			// Empty window

			{50, nil}, // only nil values to nil
			{51, nil},
		})

	runTestCases(t, Mode, nil, []testCase{
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
			name:      "mode float",
			testedBow: modeFloatBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{timeCol, valueCol},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, 42.},
						{30, 10.},
						{40, nil},
						{50, nil},
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
					[]bow.Type{bow.Int64, bow.Boolean},
					[][]interface{}{
						{10, true},
						{20, nil},
						{30, nil},
						{40, false},
						{50, true},
						{60, true},
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
					[]bow.Type{bow.Int64, bow.String},
					[][]interface{}{
						{10, "10."},
						{20, nil},
						{30, nil},
						{40, "10."},
						{50, "10."},
						{60, "test"},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
