package fill

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
)

func TestIntervalPosition(t *testing.T) {
	interval := 2.

	t.Run("no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(sparseBow, timeCol, interval, rolling.Options{})
		filled, err := r.
			Fill(IntervalPosition(timeCol)).
			Bow()
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Float64},
			[][]interface{}{
				{10., 12., 14., 15., 16., 18., 20., 22., 24., 25., 26., 28., 29.},
			})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(sparseBow, timeCol, interval, rolling.Options{Offset: 3.})
		filled, err := r.
			Fill(IntervalPosition(timeCol)).
			Bow()
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Float64},
			[][]interface{}{
				{13., 15., 16., 17., 19., 21., 23., 25., 27., 29.},
			})
		assert.Nil(t, err)
		assert.Equal(t, true, filled.Equal(expected))
	})
}
