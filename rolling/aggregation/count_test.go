package aggregation

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
)

func TestCount(t *testing.T) {
	runTestCases(t, Count, nil, []testCase{
		{
			name:      "empty",
			testedBow: emptyBow,
			expectedBow: func() bow.Bow {
				b, err := bow.NewBow(
					bow.NewSeries("time", []int64{}, nil),
					bow.NewSeries("value", []int64{}, nil),
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
