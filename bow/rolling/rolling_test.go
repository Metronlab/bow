package rolling

import (
	"fmt"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
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
		rolling, err := IntervalRolling(b, timeCol, 0, Options{})
		assert.EqualError(t, err, "intervalrolling: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("one line with offset", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 1, Options{Offset: -1})
		assert.EqualError(t, err, "intervalrolling: positive offset required")
		assert.Nil(t, rolling)
	})

	t.Run("non existing index", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		_, err := IntervalRolling(b, badCol, 1, Options{})
		assert.EqualError(t, err, fmt.Sprintf("intervalrolling: no column '%s'", badCol))
	})

	t.Run("offset too big gives valid finished iterator", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 1, Options{Offset: 9999.})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("empty bow gives valid finished iterator", func(t *testing.T) {
		b := newIntervalRollingTestBow(emptyCols)
		rolling, err := IntervalRolling(b, timeCol, 1, Options{})
		iter := rolling.(*intervalRollingIterator)
		assert.Nil(t, err)
		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})
}

func TestIntervalRolling_iterate(t *testing.T) {
	rolling, err := IntervalRolling(sparseBow, timeCol, 5, Options{})
	assert.Nil(t, err)
	assert.NotNil(t, rolling)
	iter := rolling.(*intervalRollingIterator)

	expected := []testWindow{
		{0, 10, 15, [][]interface{}{{10.}, {10}}},
		{1, 15, 20, [][]interface{}{{15., 16.}, {15, 16}}},
		{2, 20, 25, emptyCols},
		{3, 25, 30, [][]interface{}{{25., 29.}, {25, 29}}}}

	for i := 0; iter.HasNext(); i++ {
		checkTestWindow(t, iter, expected[i])
	}

	_, w, err := iter.Next()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

func TestIntervalRolling_iterate_withOffset(t *testing.T) {
	rolling, err := IntervalRolling(sparseBow, timeCol, 5, Options{Offset: 3})
	assert.Nil(t, err)
	assert.NotNil(t, rolling)
	iter := rolling.(*intervalRollingIterator)

	expected := []testWindow{
		{0, 13, 18, [][]interface{}{{15., 16.}, {15, 16}}},
		{1, 18, 23, emptyCols},
		{2, 23, 28, [][]interface{}{{25.}, {25}}},
		{3, 28, 33, [][]interface{}{{29.}, {29}}}}

	for i := 0; iter.HasNext(); i++ {
		checkTestWindow(t, iter, expected[i])
	}

	_, w, err := iter.Next()
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
	wi, w, err := iter.Next()
	assert.Equal(t, expected.windowIndex, wi)
	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Equal(t, expected.start, w.Start)
	assert.Equal(t, expected.end, w.End)

	b := newIntervalRollingTestBow(expected.cols)
	assert.Equal(t, true, w.Bow.Equal(b))
}

func newIntervalRollingTestBow(cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	types := []bow.Type{bow.Float64, bow.Int64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		panic(err)
	}
	return b
}
