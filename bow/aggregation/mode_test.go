package aggregation

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

var (
	modeFloatBow, _ = bow.NewBowFromRowBasedInterfaces(
		[]string{tc, vc},
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
)

func TestMode(t *testing.T) {
	runTestCases(t, Mode, nil, []bowTest{
		{
			Name:      "empty",
			TestedBow: empty,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries(tc, bow.Int64, []int64{}, nil),
					bow.NewSeries(vc, bow.Float64, []float64{}, nil),
				)
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			Name:      "mode",
			TestedBow: modeFloatBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{tc, vc},
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
	})
}
