package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntervalRolling_Aggregate(t *testing.T) {
	r, _ := sparseBow.IntervalRolling(timeCol, 10, RollingOptions{})
	timeAggr := NewColumnAggregation("time", Float64,
		func(col int, w Window) (interface{}, error) {
			return w.Start, nil
		})
	valueAggr := NewColumnAggregation("value", Int64,
		func(col int, w Window) (interface{}, error) {
			return int64(w.Bow.NumRows()), nil
		})
	doubleAggr := NewColumnAggregation("value", Int64,
		func(col int, w Window) (interface{}, error) {
			return int64(w.Bow.NumRows()) * 2, nil
		})

	{ // keep columns
		aggregated, err := r.
			Aggregate(timeAggr, valueAggr).
			Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"time", "value"},
			[]Type{Float64, Int64},
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
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"value", "time"},
			[]Type{Int64, Float64},
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
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Float64, Int64},
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
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"time"},
			[]Type{Float64},
			[][]interface{}{
				{10., 20.},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // more than in original
		aggregated, err := r.Aggregate(timeAggr, doubleAggr.Rename("double"), valueAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"time", "double", "value"},
			[]Type{Float64, Int64, Int64},
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
		_, err := r.Aggregate(timeAggr, NewColumnAggregation("-", Int64,
			func(col int, w Window) (interface{}, error) { return nil, nil })).Bow()
		assert.EqualError(t, err, "aggregate: no column '-'")
	}
}
