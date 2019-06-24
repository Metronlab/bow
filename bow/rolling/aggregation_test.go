package rolling

import (
	"fmt"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Aggregate(t *testing.T) {
	r, _ := IntervalRolling(sparseBow, timeCol, 10, Options{})
	timeAggr := NewColumnAggregation("time", bow.Float64,
		func(col int, w bow.Window) (interface{}, error) {
			return w.Start, nil
		})
	valueAggr := NewColumnAggregation("value", bow.Int64,
		func(col int, w bow.Window) (interface{}, error) {
			return int64(w.Bow.NumRows()), nil
		})
	doubleAggr := NewColumnAggregation("value", bow.Int64,
		func(col int, w bow.Window) (interface{}, error) {
			return int64(w.Bow.NumRows()) * 2, nil
		})

	{ // keep columns
		aggregated, err := r.
			Aggregate(timeAggr, valueAggr).
			Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"time", "value"},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{10., 20.},
				{3, 2},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // swap columns
		aggregated, err := r.
			Aggregate(valueAggr, timeAggr).
			Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"value", "time"},
			[]bow.Type{bow.Int64, bow.Float64},
			[][]interface{}{
				{3, 2},
				{10., 20.},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // rename
		aggregated, err := r.Aggregate(timeAggr.Rename("a"), valueAggr.Rename("b")).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"a", "b"},
			[]bow.Type{bow.Float64, bow.Int64},
			[][]interface{}{
				{10., 20.},
				{3, 2},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // less than in original
		aggregated, err := r.Aggregate(timeAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"time"},
			[]bow.Type{bow.Float64},
			[][]interface{}{
				{10., 20.},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // more than in original
		aggregated, err := r.Aggregate(timeAggr, doubleAggr.Rename("double"), valueAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := bow.NewBowFromColumnBasedInterfaces(
			[]string{"time", "double", "value"},
			[]bow.Type{bow.Float64, bow.Int64, bow.Int64},
			[][]interface{}{
				{10., 20.},
				{6, 4},
				{3, 2},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{
		_, err := r.Aggregate(valueAggr).Bow()
		assert.EqualError(t, err, fmt.Sprintf("aggregate: must keep column %d for intervals", timeColIdx))
	}

	{
		_, err := r.Aggregate(timeAggr, NewColumnAggregation("-", bow.Int64,
			func(col int, w bow.Window) (interface{}, error) { return nil, nil })).Bow()
		assert.EqualError(t, err, "aggregate: no column '-'")
	}
}
