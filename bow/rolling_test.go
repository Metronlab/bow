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
			10.,
			15., 16.,
			25., 29.,
		},
		{
			10,
			15, 16,
			25, 29,
		},
	})
)

func TestIntervalRolling_numWindows(t *testing.T) {
	t.Run("empty bow", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow(emptyCols), timeColIdx, 1, 0)
		assert.Nil(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("one liner bow", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0.}, {1}}),
			timeColIdx, 1, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("included last value", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0., 9.}, {1, 1}}),
			timeColIdx, 10, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("excluded last value", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0., 10.}, {1, 1}}),
			timeColIdx, 10, 0)
		assert.Nil(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("excluded first value (offset)", func(t *testing.T) {
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0., 10.}, {1, 1}}),
			timeColIdx, 10, 1)
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
	})
}

func TestIntervalRolling_init(t *testing.T) {
	t.Run("one liner", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := b.IntervalRolling(timeCol, 0, RollingOptions{})
		assert.EqualError(t, err, "intervalrolling: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("one line with offset", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{Offset: -1})
		assert.EqualError(t, err, "intervalrolling: positive offset required")
		assert.Nil(t, rolling)
	})

	t.Run("non existing index", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		_, err := b.IntervalRolling(badCol, 1, RollingOptions{})
		assert.EqualError(t, err, fmt.Sprintf("intervalrolling: no column '%s'", badCol))
	})

	t.Run("offset too big gives valid finished iterator", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{Offset: 9999.})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
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
		{0, 10, 15, [][]interface{}{{10.}, {10}}},
		{1, 15, 20, [][]interface{}{{15., 16.}, {15, 16}}},
		{2, 20, 25, emptyCols},
		{3, 25, 30, [][]interface{}{{25., 29.}, {25, 29}}}}

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
		{0, 13, 18, [][]interface{}{{15., 16.}, {15, 16}}},
		{1, 18, 23, emptyCols},
		{2, 23, 28, [][]interface{}{{25.}, {25}}},
		{3, 28, 33, [][]interface{}{{29.}, {29}}}}

	for i := 0; iter.hasNext(); i++ {
		checkTestWindow(t, iter, expected[i])
	}

	_, w, err := iter.next()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

type testWindow struct {
	windowIndex int
	start       float64
	end         float64
	cols        [][]interface{}
}

func checkTestWindow(t *testing.T, iter *intervalRollingIterator, expected testWindow) {
	wi, w, err := iter.next()
	assert.Equal(t, expected.windowIndex, wi)
	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Equal(t, expected.start, w.Start)
	assert.Equal(t, expected.end, w.End)

	b := newIntervalRollingTestBow(expected.cols)
	assert.Equal(t, true, w.Bow.Equal(b))
}

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

func newIntervalRollingTestBow(cols [][]interface{}) Bow {
	colNames := []string{timeCol, valueCol}
	types := []Type{Float64, Int64}
	b, err := NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
