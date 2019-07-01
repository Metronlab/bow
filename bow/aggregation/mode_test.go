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
			{10, 10.1}, // same value window
			{11, 10.1},

			{20, 42.1}, // most occurrences to 42
			{21, 42.1},
			{22, 10.1},

			{30, nil},
			{31, nil}, // most occurrences to nil
			{32, 10.1},
		})
)

func TestMode(t *testing.T) {
	runTestCases(t, Mode, []bowTest{
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
						{10, 10.1},
						{20, 42.1},
						{30, 10.1},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
