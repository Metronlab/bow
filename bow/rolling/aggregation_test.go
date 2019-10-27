package rolling

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Aggregate(t *testing.T) {
	b, _ := bow.NewBowFromColumnBasedInterfaces([]string{timeCol, valueCol}, []bow.Type{bow.Int64, bow.Float64}, [][]interface{}{
		{10, 15, 16, 25, 29},
		{1.0, 1.5, 1.6, 2.5, 2.9},
	})
	r, _ := IntervalRolling(b, timeCol, 10, Options{})

	timeAggr := NewColumnAggregation(timeCol, false, bow.Int64,
		func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		})
	valueAggr := NewColumnAggregation(valueCol, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			return float64(w.Bow.NumRows()), nil
		})
	doubleAggr := NewColumnAggregation(valueCol, false, bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			return float64(w.Bow.NumRows()) * 2, nil
		})

	t.Run("keep columns", func(t *testing.T) {
		aggregated, err := r.
			Aggregate(timeAggr, valueAggr).
			Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{timeCol, valueCol},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20},
				{3., 2.},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("swap columns", func(t *testing.T) {
		aggregated, err := r.
			Aggregate(valueAggr, timeAggr).
			Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{valueCol, timeCol},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{3., 2.},
				{10, 20},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("rename columns", func(t *testing.T) {
		aggregated, err := r.Aggregate(timeAggr.Rename("a"), valueAggr.Rename("b")).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"a", "b"},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{10, 20},
				{3., 2.},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("less than in original", func(t *testing.T) {
		aggregated, err := r.Aggregate(timeAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Int64},
			[][]interface{}{
				{10, 20},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("more than in original", func(t *testing.T) {
		aggregated, err := r.Aggregate(timeAggr, doubleAggr.Rename("double"), valueAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{timeCol, "double", valueCol},
			[]bow.Type{bow.Int64, bow.Float64, bow.Float64},
			[][]interface{}{
				{10, 20},
				{6., 4.},
				{3., 2.},
			})
		assert.True(t, aggregated.Equal(expected))
	})

	t.Run("missing interval column", func(t *testing.T) {
		_, err := r.Aggregate(valueAggr).Bow()
		assert.EqualError(t, err, fmt.Sprintf("aggregate: must keep interval column '%s'", timeCol))
	})

	t.Run("invalid column", func(t *testing.T) {
		_, err := r.Aggregate(timeAggr, NewColumnAggregation("-", false, bow.Int64,
			func(col int, w bow.Window) (interface{}, error) { return nil, nil })).Bow()
		assert.EqualError(t, err, "aggregate: no column '-'")
	})
}
