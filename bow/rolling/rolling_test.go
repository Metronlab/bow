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
)

func TestIntervalRolling_NumWindows(t *testing.T) {
	t.Run("empty bow", func(t *testing.T) {
		r, _ := IntervalRolling(newIntervalRollingTestBow(emptyCols), timeCol, 1, Options{})
		n, err := r.NumWindows()
		assert.Nil(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("one liner bow", func(t *testing.T) {
		r, _ := IntervalRolling(newIntervalRollingTestBow([][]interface{}{
			{0.}, {1},
		}), timeCol, 1, Options{})
		n, err := r.NumWindows()
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("points in same window", func(t *testing.T) {
		r, _ := IntervalRolling(newIntervalRollingTestBow([][]interface{}{
			{0., 9.}, {1, 1},
		}), timeCol, 10, Options{})
		n, err := r.NumWindows()
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("excluded point goes in next window", func(t *testing.T) {
		r, _ := IntervalRolling(newIntervalRollingTestBow([][]interface{}{
			{0., 10.}, {1, 1},
		}), timeCol, 10, Options{})
		n, err := r.NumWindows()
		assert.Nil(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("offset puts first value in preceding window", func(t *testing.T) {
		r, _ := IntervalRolling(newIntervalRollingTestBow([][]interface{}{
			{0., 9.}, {1, 1},
		}), timeCol, 10, Options{Offset: 1})
		n, err := r.NumWindows()
		assert.Nil(t, err)
		assert.Equal(t, 2, n)
	})
}

func TestIntervalRolling_iterator_init(t *testing.T) {
	t.Run("interval < 0", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 0, Options{})
		assert.EqualError(t, err, "intervalrolling: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("interval == 0", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 0, Options{})
		assert.EqualError(t, err, "intervalrolling: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("offset < 0", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 1, Options{Offset: -1})
		assert.EqualError(t, err, "intervalrolling: positive offset required")
		assert.Nil(t, rolling)
	})

	t.Run("offset == 0", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 1, Options{Offset: 0})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
	})

	t.Run("offset == interval", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 1, Options{Offset: 1})
		assert.EqualError(t, err, "intervalrolling: offset must be lower than interval")
		assert.Nil(t, rolling)
	})

	t.Run("offset > interval", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		rolling, err := IntervalRolling(b, timeCol, 1, Options{Offset: 2})
		assert.EqualError(t, err, "intervalrolling: offset must be lower than interval")
		assert.Nil(t, rolling)
	})

	t.Run("non existing index", func(t *testing.T) {
		b := newIntervalRollingTestBow([][]interface{}{{0.}, {1}})
		_, err := IntervalRolling(b, badCol, 1, Options{})
		assert.EqualError(t, err, fmt.Sprintf("intervalrolling: no column '%s'", badCol))
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
	interval := 5.
	b := newIntervalRollingTestBow([][]interface{}{
		{
			12.,
			15., 16.,
			25., 29.,
		},
		{
			12,
			15, 16,
			25, 29,
		},
	})

	t.Run("no option", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 10, 15, 0, [][]interface{}{{12.}, {12}}},
			{1, 15, 20, 1, [][]interface{}{{15., 16.}, {15, 16}}},
			{2, 20, 25, -1, emptyCols},
			{3, 25, 30, 3, [][]interface{}{{25., 29.}, {25, 29}}}}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("with offset falling before first point", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{Offset: 1})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 11, 16, 0, [][]interface{}{{12., 15.}, {12, 15}}},
			{1, 16, 21, 2, [][]interface{}{{16.}, {16}}},
			{2, 21, 26, 3, [][]interface{}{{25.}, {25}}},
			{3, 26, 31, 4, [][]interface{}{{29.}, {29}}},
		}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("with offset falling at first point", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{Offset: 2})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 12, 17, 0, [][]interface{}{{12., 15., 16.}, {12, 15, 16}}},
			{1, 17, 22, -1, emptyCols},
			{2, 22, 27, 3, [][]interface{}{{25.}, {25}}},
			{3, 27, 32, 4, [][]interface{}{{29.}, {29}}},
		}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("with offset falling after first point", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{Offset: 3})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 8, 13, 0, [][]interface{}{{12.}, {12}}},
			{1, 13, 18, 1, [][]interface{}{{15., 16.}, {15, 16}}},
			{2, 18, 23, -1, emptyCols},
			{3, 23, 28, 3, [][]interface{}{{25.}, {25}}},
			{4, 28, 33, 4, [][]interface{}{{29.}, {29}}},
		}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})
}

type testWindow struct {
	windowIndex int
	start       float64
	end         float64
	firstIndex  int
	cols        [][]interface{}
}

func checkTestWindow(t *testing.T, iter *intervalRollingIterator, expected testWindow) {
	wi, w, err := iter.Next()
	assert.Equal(t, expected.windowIndex, wi)
	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Equal(t, expected.start, w.Start)
	assert.Equal(t, expected.end, w.End)
	assert.Equal(t, expected.firstIndex, w.FirstIndex)

	b := newIntervalRollingTestBow(expected.cols)
	assert.True(t, w.Bow.Equal(b))
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
