package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	timeCol, timeColIdx = "time", 0
	valueCol            = "value"
	badCol              = "badcol"

	emptyCols = [][]interface{}{{}, {}}
	sparseBow = newIntervalRollingTestBow([][]interface{}{
		{
			10,
			15, 16,
			25,
		},
		{
			10.1,
			15.1, 16.1,
			25.1,
		},
	})
)

func TestIntervalRolling_numWindows(t *testing.T) {
	t.Run("empty bow", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow(emptyCols), timeColIdx, 1, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
	})

	t.Run("one liner bow", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0}, {0.1}}),
			timeColIdx, 1, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	})

	t.Run("included last value", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0, 9}, {0.1, 0.1}}),
			timeColIdx, 10, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	})

	t.Run("excluded last value", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0, 10}, {0.1, 0.1}}),
			timeColIdx, 10, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(2), n)
	})

	t.Run("excluded first value (offset)", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0, 10}, {0.1, 0.1}}),
			timeColIdx, 10, 1)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	})
}

func TestIntervalRolling_init(t *testing.T) {
	t.Run("one liner", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		rolling, err := b.IntervalRolling(timeCol, 0, RollingOptions{})
		assert.EqualError(t, err, "intervalrolling: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("one line with offset", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{Offset: -1})
		assert.EqualError(t, err, "intervalrolling: positive offset required")
		assert.Nil(t, rolling)
	})

	t.Run("non existing index", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		_, err := b.IntervalRolling(badCol, 1, RollingOptions{})
		assert.EqualError(t, err, fmt.Sprintf("intervalrolling: no column '%s'", badCol))
	})

	t.Run("offset too big gives valid finished iterator", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{Offset: 1e9})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
		assert.NotNil(t, iter)
		_, w, err := iter.next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("empty bow gives valid finished iterator", func(t *testing.T) {
		b := newIntervalRollingTestBow(emptyCols)
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
		_, w, err := iter.next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})
}

func TestIntervalRolling_iterate(t *testing.T) {
	rolling, err := sparseBow.IntervalRolling(timeCol, 5, RollingOptions{})
	assert.Nil(t, err)
	assert.NotNil(t, rolling)
	iter := rolling.(*intervalRollingIterator)

	expected := []testWindow{
		{0, 10, 14, [][]interface{}{{10}, {10.1}}},
		{1, 15, 19, [][]interface{}{{15, 16}, {15.1, 16.1}}},
		{2, 20, 24, emptyCols},
		{3, 25, 29, [][]interface{}{{25}, {25.1}}}}

	for i := 0; iter.hasNext(); i++ {
		checkTestWindow(t, iter, expected[i])
	}

	_, w, err := iter.next()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

func TestIntervalRolling_iterate_withOffset(t *testing.T) {
	rolling, err := sparseBow.IntervalRolling(timeCol, 5, RollingOptions{Offset: 3})
	assert.Nil(t, err)
	assert.NotNil(t, rolling)
	iter := rolling.(*intervalRollingIterator)

	expected := []testWindow{
		{0, 13, 17, [][]interface{}{{15, 16}, {15.1, 16.1}}},
		{1, 18, 22, emptyCols},
		{2, 23, 27, [][]interface{}{{25}, {25.1}}}}

	for i := 0; iter.hasNext(); i++ {
		checkTestWindow(t, iter, expected[i])
	}

	_, w, err := iter.next()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

type testWindow struct {
	windowIndex int64
	start       int64
	end         int64
	cols        [][]interface{}
}

func checkTestWindow(t *testing.T, iter *intervalRollingIterator, expected testWindow) {
	wi, w, err := iter.next()
	assert.Equal(t, expected.windowIndex, wi)
	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Equal(t, int64(expected.start), w.Start)
	assert.Equal(t, int64(expected.end), w.End)

	b := newIntervalRollingTestBow(expected.cols)
	assert.Equal(t, true, w.Bow.Equal(b))
}

func TestIntervalRolling_Aggregate(t *testing.T) {
	r, _ := sparseBow.IntervalRolling(timeCol, 10, RollingOptions{})
	timeAggr := NewColumnAggregation("time", Int64,
		func(col int, w Window) (interface{}, error) {
			return w.Start, nil
		})
	valueAggr := NewColumnAggregation("value", Float64,
		func(col int, w Window) (interface{}, error) {
			return float64(w.Bow.NumRows()), nil
		})
	doubleAggr := NewColumnAggregation("value", Float64,
		func(col int, w Window) (interface{}, error) {
			return float64(w.Bow.NumRows()) * 2, nil
		})

	{ // keep columns
		aggregated, err := r.
			Aggregate(timeAggr, valueAggr).
			Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"time", "value"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{10, 20},
				{3.0, 1.0},
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
			[]Type{Float64, Int64},
			[][]interface{}{
				{3.0, 1.0},
				{10, 20},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // rename
		aggregated, err := r.Aggregate(timeAggr.Rename("a"), valueAggr.Rename("b")).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"a", "b"},
			[]Type{Int64, Float64},
			[][]interface{}{
				{10, 20},
				{3.0, 1.0},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // less than in original
		aggregated, err := r.Aggregate(timeAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"time"},
			[]Type{Int64},
			[][]interface{}{
				{10, 20},
			})
		assert.Equal(t, true, aggregated.Equal(expected))
	}

	{ // more than in original
		aggregated, err := r.Aggregate(timeAggr, doubleAggr.Rename("double"), valueAggr).Bow()
		assert.Nil(t, err)
		assert.NotNil(t, aggregated)
		expected, _ := NewBowFromColumnBasedInterfaces(
			[]string{"time", "double", "value"},
			[]Type{Int64, Float64, Float64},
			[][]interface{}{
				{10, 20},
				{6.0, 2.0},
				{3.0, 1.0},
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

func newIntervalRollingTestBow(cols [][]interface{}) Bow {
	colNames := []string{timeCol, valueCol}
	types := []Type{Int64, Float64}
	b, err := NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
