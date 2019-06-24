package fill

import (
	"testing"

	"git.metronlab.com/backend_libraries/go-bow/bow"
	"git.metronlab.com/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
)

var (
	timeCol  = "time"
	valueCol = "value"
	//badCol   = "badcol"

	//emptyCols = [][]interface{}{{}, {}}
	sparseBow = newInputTestBow([][]interface{}{
		{10., 15., 16., 25., 29.},
		{.10, .15, .16, .25, .29},
	})
)

func TestStepPrevious(t *testing.T) {
	rollInterval := 2.

	t.Run("no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(sparseBow, timeCol, rollInterval, rolling.Options{})
		filled, err := r.
			Fill(IntervalPosition(timeCol), StepPrevious(valueCol)).
			Bow()
		expected := newOutputTestBow([][]interface{}{
			{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
			{.10, .10, .10, .15, .16, .16, .16, .16, .16, .25, .25, .25, .29},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(sparseBow, timeCol, rollInterval, rolling.Options{Offset: 3.})
		filled, err := r.
			Fill(IntervalPosition(timeCol), StepPrevious(valueCol)).
			Bow()
		expected := newOutputTestBow([][]interface{}{
			{13., 15., 16., 17., 19., 21., 23., 25., 27., 29.},
			{.10, .15, .16, .16, .16, .16, .16, .25, .25, .29},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected))
	})

}

func newInputTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Float64, bow.Float64}
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
