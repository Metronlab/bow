package fill

import (
	"git.metronlab.com/backend_libraries/go-bow/bow"
)

var (
	timeCol  = "time"
	valueCol = "value"
	badCol   = "badcol"

	emptyCols = [][]interface{}{{}, {}}
	sparseBow = newInputTestBow([][]interface{}{
		{
			10.,
			15., 16.,
			25., 29.,
		},
		{
			10,
			15, 16,
			25, 29,
		},
	})
)

// func TestStepPrevious(t *testing.T) {
// 	rollInterval := 10.
// 	fillInterval := 2.
// 	r, _ := sparseBow.IntervalRolling(timeCol, rollInterval, bow.RollingOptions{})
// 	timeInterp := bow.NewColumnInterpolation(timeCol, []bow.Type{bow.Int64, bow.Float64},
// 		func(colIndex int, neededPos float64, w bow.Window) (interface{}, error) {
// 			return nil, nil
// 		})

// 	filled, err := r.
// 		Fill(fillInterval, timeInterp, FillStepPrevious(valueCol)).
// 		Bow()
// 	assert.Nil(t, err)

// 	expected, _ := bow.NewBowFromColumnBasedInterfaces(
// 		[]string{"time", "value"},
// 		[]bow.Type{bow.Float64, bow.Int64},
// 		[][]interface{}{
// 			{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
// 			{10, 10, 10, 15, 16, 16, 16, 16, 16, 25, 25, 25, 29},
// 		})
// 	assert.Equal(t, true, filled.Equal(expected))
// 	// todo: empty start, ...
// }

func newInputTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Float64, bow.Int64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}

func newOutputTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Float64, bow.Float64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
