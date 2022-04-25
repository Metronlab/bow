package interpolation

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLinear(t *testing.T) {
	var rollInterval int64 = 2

	ascendantLinearTestBow, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
		{10, 15, 16, 25, 29},
		{.10, .15, .16, .25, .29},
	})

	descendantLinearTestBow, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
		{10, 15, 16, 25, 29},
		{.30, .25, .24, .15, .11},
	})

	t.Run("ascendant no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(ascendantLinearTestBow, timeCol, rollInterval, rolling.Options{})
		filled, err := r.
			Interpolate(WindowStart(timeCol), Linear(valueCol)).
			Bow()

		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 12, 14, 15, 16, 18, 20, 22, 24, 25, 26, 28, 29},
			{.10, .12, .14, .15, .16, .18, .20, .22, .24, .25, .26, .27999999999999997, .29},
		})

		assert.Nil(t, err)
		assert.Equal(t, expected.String(), filled.String(),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("descendant no options", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(descendantLinearTestBow, timeCol, rollInterval, rolling.Options{})
		filled, err := r.
			Interpolate(WindowStart(timeCol), Linear(valueCol)).
			Bow()

		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{10, 12, 14, 15, 16, 18, 20, 22, 24, 25, 26, 28, 29},
			{.3, .27999999999999997, .26, .25, .24, .22, .2, .18, .16, .15, .13999999999999999, .12, .11},
		})

		assert.Nil(t, err)
		assert.Equal(t, expected.String(), filled.String(),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("ascendant with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(ascendantLinearTestBow, timeCol, rollInterval, rolling.Options{Offset: 3.})
		filled, err := r.
			Interpolate(WindowStart(timeCol), Linear(valueCol)).
			Bow()

		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{9, 10, 11, 13, 15, 16, 17, 19, 21, 23, 25, 27, 29},
			{nil, .1, .11, .13, .15, .16, .17, .19, .21000000000000002, .22999999999999998, .25, .27, .29},
		})

		assert.Nil(t, err)
		assert.Equal(t, expected.String(), filled.String(),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("descendant with offset", func(t *testing.T) {
		r, _ := rolling.IntervalRolling(descendantLinearTestBow, timeCol, rollInterval, rolling.Options{Offset: 3.})
		filled, err := r.
			Interpolate(WindowStart(timeCol), Linear(valueCol)).
			Bow()

		expected, _ := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
			{9, 10, 11, 13, 15, 16, 17, 19, 21, 23, 25, 27, 29},
			{nil, .3, .29, .27, .25, .24, .22999999999999998, .21, .19, .16999999999999998, .15, .13, .11},
		})

		assert.Nil(t, err)
		assert.Equal(t, expected.String(), filled.String(),
			fmt.Sprintf("expected %s\nactual %s", expected.String(), filled.String()))
	})

	t.Run("string error", func(t *testing.T) {
		b, err := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.String}, [][]interface{}{
			{10, 15},
			{"test", "test2"},
		})
		require.NoError(t, err)
		r, _ := rolling.IntervalRolling(b, timeCol, rollInterval, rolling.Options{})
		_, err = r.
			Interpolate(WindowStart(timeCol), Linear(valueCol)).
			Bow()
		assert.EqualError(t, err, "intervalRolling.validateInterpolation: accepts types [int64 float64], got type utf8")
	})

	t.Run("bool error", func(t *testing.T) {
		b, err := bow.NewBowFromColBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Boolean}, [][]interface{}{
			{10, 15},
			{true, false},
		})
		require.NoError(t, err)
		r, _ := rolling.IntervalRolling(b, timeCol, rollInterval, rolling.Options{})
		res, err := r.
			Interpolate(WindowStart(timeCol), Linear(valueCol)).
			Bow()
		assert.EqualError(t, err, "intervalRolling.validateInterpolation: accepts types [int64 float64], got type bool",
			"have res: %v", res)
	})
}
