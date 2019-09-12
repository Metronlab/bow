package aggregation

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestFirst(t *testing.T) {
	runTestCases(t, First, nil, []bowTest{
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
			Name:      "sparse",
			TestedBow: sparseFloatBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, nil},
						{30, nil},
						{40, 10.},
						{50, 10.},
						{60, 10.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			Name: "sparse bool",
			TestedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Bool},
					[][]interface{}{
						{10, true}, // partially valid window
						{11, nil},
						{20, nil}, // only invalid window

						// empty window

						{40, nil}, // partially valid with start of window invalid
						{41, true},
						{50, true}, // valid with two values on start of window
						{51, false},
						{61, false}, // valid with two values NOT on start of window
						{69, true},
					})

				assert.NoError(t, err)
				return b
			}(),
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Bool},
					[][]interface{}{
						{10, true},
						{20, nil},
						{30, nil},
						{40, true},
						{50, true},
						{60, false},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}

func TestLast(t *testing.T) {
	runTestCases(t, Last, nil, []bowTest{
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
			Name:      "sparse",
			TestedBow: sparseFloatBow,
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					[][]interface{}{
						{10, 10.},
						{20, nil},
						{30, nil},
						{40, 10.},
						{50, 20.},
						{60, 20.},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
		{
			Name: "sparse bool",
			TestedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Bool},
					[][]interface{}{
						{10, true}, // partially valid window
						{11, nil},
						{20, nil}, // only invalid window

						// empty window

						{40, nil}, // partially valid with start of window invalid
						{41, true},
						{50, true}, // valid with two values on start of window
						{51, false},
						{61, false}, // valid with two values NOT on start of window
						{69, true},
					})

				assert.NoError(t, err)
				return b
			}(),
			ExpectedBow: func() bow.Bow {
				b, err := bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Bool},
					[][]interface{}{
						{10, true},
						{20, nil},
						{30, nil},
						{40, true},
						{50, false},
						{60, true},
					})
				assert.NoError(t, err)
				return b
			}(),
		},
	})
}
