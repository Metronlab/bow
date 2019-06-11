package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	timeCol  = 0
	valueCol = 1
	badCol   = 99

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
	{
		n, err := numWindows(newIntervalRollingTestBow(emptyCols), timeCol, 1, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), n)
	}
	{
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0}, {0.1}}),
			timeCol, 1, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	}
	{
		fmt.Println("included last value")
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0, 9}, {0.1, 0.1}}),
			timeCol, 10, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	}
	{
		fmt.Println("excluded last value")
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0, 10}, {0.1, 0.1}}),
			timeCol, 10, 0)
		assert.Nil(t, err)
		assert.Equal(t, int64(2), n)
	}
	{
		fmt.Println("excluded first value (offset)")
		n, err := numWindows(newIntervalRollingTestBow([][]interface{}{
			{0, 10}, {0.1, 0.1}}),
			timeCol, 10, 1)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), n)
	}
}

func TestIntervalRolling_init(t *testing.T) {
	{
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		rolling, err := b.IntervalRolling(timeCol, 0, RollingOptions{})
		assert.EqualError(t, err, "strictly positive interval required")
		assert.Nil(t, rolling)
	}
	{
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{Offset: -1})
		assert.EqualError(t, err, "positive offset required")
		assert.Nil(t, rolling)
	}
	{
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		_, err := b.IntervalRolling(badCol, 1, RollingOptions{})
		assert.EqualError(t, err, fmt.Sprintf("no column at index %d", badCol))
	}
	{
		fmt.Println("offset too big gives valid finished iterator")
		b := newIntervalRollingTestBow([][]interface{}{{0}, {0.1}})
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{Offset: 1e9})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
		assert.NotNil(t, iter)
		_, w, err := iter.next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	}
	{
		fmt.Println("empty bow gives valid finished iterator")
		b := newIntervalRollingTestBow(emptyCols)
		rolling, err := b.IntervalRolling(timeCol, 1, RollingOptions{})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
		_, w, err := iter.next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	}
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
	aggregated, err := r.
		Aggregate(
			ColumnAggregation{
				Type: Int64,
				Func: func(col int, w Window) (interface{}, error) {
					return w.Start, nil
				}}.SetName("start"),
			ColumnAggregation{
				Type: Int64,
				Func: func(col int, w Window) (interface{}, error) {
					return w.Start * 2, nil
				}}.SetName("startDouble"),
		).
		Bow()
	assert.Nil(t, err)
	assert.NotNil(t, aggregated)

	expected, _ := NewBowFromColumnBasedInterfaces(
		[]string{"start", "startDouble"},
		[]Type{Int64, Int64},
		[][]interface{}{
			{10, 20},
			{20, 40}})
	assert.Equal(t, true, aggregated.Equal(expected))
}

func TestIntervalRolling_Aggregate_mismatch(t *testing.T) {
	r, _ := sparseBow.IntervalRolling(timeCol, 10, RollingOptions{})
	aggr := ColumnAggregation{
		Type: Int64,
		Func: func(col int, w Window) (interface{}, error) {
			return 0, nil
		}}
	{
		_, err := r.Aggregate(aggr).Bow()
		assert.EqualError(t, err, "mismatch between columns and aggregations")
	}
	{
		_, err := r.Aggregate(aggr, aggr, aggr).Bow()
		assert.EqualError(t, err, "mismatch between columns and aggregations")
	}
}

func newIntervalRollingTestBow(cols [][]interface{}) Bow {
	colNames := []string{"time", "value"}
	types := []Type{Int64, Float64}
	b, err := NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
