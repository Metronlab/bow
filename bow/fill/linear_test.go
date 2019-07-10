package fill

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
)

var (
	ascendantLinearTestBow = newInputTestBow([][]interface{}{
		{10., 15., 16., 25., 29.},
		{.10, .15, .16, .25, .29},
	})
	descendantLinearTestBow = newInputTestBow([][]interface{}{
		{10., 15., 16., 25., 29.},
		{.30, .25, .24, .15, .11},
	})
)

func TestLinear(t *testing.T) {
	rollInterval := 2.

	t.Run("ascendant no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(ascendantLinearTestBow, timeCol, rollInterval, rolling.Options{})
		filled, err := r.
			Fill(IntervalPosition(timeCol), Linear(valueCol)).
			Bow()
		expected := newOutputTestBow([][]interface{}{
			{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
			{.10, .12, .14, .15, .16, .18, .20, .22, .24, .25, .26, .27999999999999997, .29},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected), expected.String(), filled.String())
	})

	t.Run("descendant no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(descendantLinearTestBow, timeCol, rollInterval, rolling.Options{})
		filled, err := r.
			Fill(IntervalPosition(timeCol), Linear(valueCol)).
			Bow()
		expected := newOutputTestBow([][]interface{}{
			{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
			{.3, .27999999999999997, .26, .25, .24, .22, .2, .18, .16, .15, .13999999999999999, .12, .11},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected), expected.String(), filled.String())
	})

	t.Run("ascendant with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(ascendantLinearTestBow, timeCol, rollInterval, rolling.Options{Offset: 3.})
		filled, err := r.
			Fill(IntervalPosition(timeCol), Linear(valueCol)).
			Bow()
		expected := newOutputTestBow([][]interface{}{
			{13., 15., 16., 17., 19., 21., 23., 25., 27., 29.},
			{.13, .15, .16, .17, .19, .21000000000000002, .22999999999999998, .25, .27, .29},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected), expected.String(), filled.String())
	})

	t.Run("descendant with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(descendantLinearTestBow, timeCol, rollInterval, rolling.Options{Offset: 3.})
		filled, err := r.
			Fill(IntervalPosition(timeCol), Linear(valueCol)).
			Bow()
		expected := newOutputTestBow([][]interface{}{
			{13., 15., 16., 17., 19., 21., 23., 25., 27., 29.},
			{.27, .25, .24, .22999999999999998, .21, .19, .16999999999999998, .15, .13, .11},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected), expected.String(), filled.String())
	})

}
