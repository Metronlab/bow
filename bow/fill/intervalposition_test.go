package fill

import (
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
	"github.com/stretchr/testify/assert"
)

func TestIntervalPosition(t *testing.T) {
	var interval int64 = 2
	b, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol}, []bow.Type{bow.Int64}, [][]interface{}{
		{10, 13},
	})

	t.Run("no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{})
		filled, err := r.
			Fill(IntervalPosition(timeCol)).
			Bow()
		expected, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol}, []bow.Type{bow.Int64}, [][]interface{}{
			{10, 12, 13},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{Offset: 1.})
		filled, err := r.
			Fill(IntervalPosition(timeCol)).
			Bow()
		expected, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol}, []bow.Type{bow.Int64}, [][]interface{}{
			{9, 10, 11, 13},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected))
	})
}
