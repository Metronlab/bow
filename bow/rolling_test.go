package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	timeIndex   = 0
	badIndex    = 99
	emptySeries = [][]interface{}{{}, {}}
)

func TestIntervalRolling_Creation(t *testing.T) {
	{
		b := newIntervalRollingTestBow([][]interface{}{{1}, {1.1}})
		iter, err := b.IntervalRolling(timeIndex, 0, IntervalRollingOptions{Offset: 0})
		assert.EqualError(t, err, "strictly positive interval required")
		assert.Nil(t, iter)
	}
	{
		b := newIntervalRollingTestBow([][]interface{}{{1}, {1.1}})
		iter, err := b.IntervalRolling(timeIndex, 1, IntervalRollingOptions{Offset: -1})
		assert.EqualError(t, err, "positive offset required")
		assert.Nil(t, iter)
	}
	{
		b := newIntervalRollingTestBow([][]interface{}{{1}, {1.1}})
		_, err := b.IntervalRolling(badIndex, 1, IntervalRollingOptions{Offset: 0})
		assert.EqualError(t, err, fmt.Sprintf("no column at index %d", badIndex))
	}
	{ // an offset too big gives a valid finished iterator
		b := newIntervalRollingTestBow([][]interface{}{{1}, {1.1}})
		iter, err := b.IntervalRolling(timeIndex, 1, IntervalRollingOptions{Offset: 1e9})
		assert.Nil(t, err)
		assert.NotNil(t, iter)
		w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	}
	{ // an empty bow gives a valid finished iterator
		b := newIntervalRollingTestBow(emptySeries)
		iter, err := b.IntervalRolling(timeIndex, 1, IntervalRollingOptions{Offset: 0})
		assert.Nil(t, err)
		w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	}
}

func TestIntervalRolling_Iteration(t *testing.T) {
	b := newIntervalRollingTestBow([][]interface{}{
		{
			9,
			10, 11,
			20,
		},
		{
			0.91,
			1.01, 1.11,
			2.01,
		},
	})
	iter, err := b.IntervalRolling(timeIndex, 5, IntervalRollingOptions{Offset: 10})
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	fmt.Println("full bow")
	fmt.Println(b)

	w, err := iter.Next()
	assert.NotNil(t, w)
	assert.Nil(t, err)
	fmt.Println("window", w)
	fmt.Println(w.Bow)
	assert.Equal(t, int64(10), w.Start)
	assert.Equal(t, int64(14), w.End)
	assert.Equal(t, true, w.Bow.Equal(
		newIntervalRollingTestBow([][]interface{}{{10, 11}, {1.01, 1.11}})))

	w, err = iter.Next()
	assert.NotNil(t, w)
	assert.Nil(t, err)
	fmt.Println("window", w)
	fmt.Println(w.Bow)
	assert.Equal(t, int64(15), w.Start)
	assert.Equal(t, int64(19), w.End)
	assert.Equal(t, true, w.Bow.Equal(
		newIntervalRollingTestBow(emptySeries)))

	w, err = iter.Next()
	assert.NotNil(t, w)
	assert.Nil(t, err)
	fmt.Println("window", w)
	fmt.Println(w.Bow)
	assert.Equal(t, int64(20), w.Start)
	assert.Equal(t, int64(24), w.End)
	assert.Equal(t, true, w.Bow.Equal(
		newIntervalRollingTestBow([][]interface{}{{20}, {2.01}})))

	w, err = iter.Next()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

func newIntervalRollingTestBow(series [][]interface{}) Bow {
	colNames := []string{"time", "value"}
	types := []Type{Int64, Float64}
	b, err := NewBowFromColumnBasedInterfaces(colNames, types, series)
	if err != nil {
		panic(err)
	}
	return b
}
