package fill

import (
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
)

func TestIntervalPosition(t *testing.T) {
	var interval int64 = 2
	b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol}, []bow.Type{bow.Int64}, [][]interface{}{
		{10, 13},
	})

	t.Run("no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{})
		filled, err := r.
			Fill(WindowStart(timeCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol}, []bow.Type{bow.Int64}, [][]interface{}{
			{10, 12, 13},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected))
	})

	t.Run("with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(b, timeCol, interval, rolling.Options{Offset: 1.})
		filled, err := r.
			Fill(WindowStart(timeCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol}, []bow.Type{bow.Int64}, [][]interface{}{
			{9, 10, 11, 13},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected))
	})
}
