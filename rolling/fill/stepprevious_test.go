package fill

import (
	"fmt"
	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	timeCol  = "time"
	valueCol = "value"
)

func TestStepPrevious(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 13},
			{1.0, 1.3},
		})
		r, _ := rolling.IntervalRolling(b, nil, timeCol, 2, rolling.Options{})
		filled, err := r.
			Fill(WindowStart(timeCol), StepPrevious(valueCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 12, 13},
			{1.0, 1.0, 1.3},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("bool", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Bool}, [][]interface{}{
			{10, 13},
			{true, false},
		})
		r, _ := rolling.IntervalRolling(b, nil, timeCol, 2, rolling.Options{})
		filled, err := r.
			Fill(WindowStart(timeCol), StepPrevious(valueCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Bool}, [][]interface{}{
			{10, 12, 13},
			{true, true, false},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("string", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.String}, [][]interface{}{
			{10, 13},
			{"test", "test2"},
		})
		r, _ := rolling.IntervalRolling(b, nil, timeCol, 2, rolling.Options{})
		filled, err := r.
			Fill(WindowStart(timeCol), StepPrevious(valueCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.String}, [][]interface{}{
			{10, 12, 13},
			{"test", "test", "test2"},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("with offset", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 13},
			{1.0, 1.3},
		})
		r, _ := rolling.IntervalRolling(b, nil, timeCol, 2, rolling.Options{Offset: 1})
		filled, err := r.
			Fill(WindowStart(timeCol), StepPrevious(valueCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{9, 10, 11, 13},
			{nil, 1.0, 1.0, 1.3},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("with nils", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 11, 13, 15},
			{1.0, nil, nil, 1.5},
		})
		r, _ := rolling.IntervalRolling(b, nil, timeCol, 2, rolling.Options{})
		filled, err := r.
			Fill(WindowStart(timeCol), StepPrevious(valueCol)).
			Bow()
		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 11, 12, 13, 14, 15},
			{1.0, nil, 1.0, nil, 1.0, 1.5},
		})
		assert.Nil(t, err)
		assert.True(t, filled.Equal(expected),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})
}
