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
	interval := 5.1
	b := newIntervalRollingTestBow([][]interface{}{
		{
			12.2,
			15.5, 16.6,
			25.5, 29.9,
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
			{0, 10.2, 15.299999999999999, 0, [][]interface{}{{12.2}, {12}}},
			{1, 15.299999999999999, 20.4, 1, [][]interface{}{{15.5, 16.6}, {15, 16}}},
			{2, 20.4, 25.5, -1, emptyCols},
			{3, 25.5, 30.6, 3, [][]interface{}{{25.5, 29.9}, {25, 29}}},
		}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("with inclusive windows", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{Inclusive: true})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 10.2, 15.299999999999999, 0, [][]interface{}{{12.2}, {12}}},
			{1, 15.299999999999999, 20.4, 1, [][]interface{}{{15.5, 16.6}, {15, 16}}},
			{2, 20.4, 25.5, 3, [][]interface{}{{25.5}, {25}}},
			{3, 25.5, 30.6, 3, [][]interface{}{{25.5, 29.9}, {25, 29}}},
		}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("with offset falling before first point", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{Offset: 0.1})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 10.299999999999999, 15.399999999999999, 0, [][]interface{}{{12.2}, {12}}},
			{1, 15.399999999999999, 20.5, 1, [][]interface{}{{15.5, 16.6}, {15, 16}}},
			{2, 20.5, 25.6, 3, [][]interface{}{{25.5}, {25}}},
			{3, 25.6, 30.700000000000003, 4, [][]interface{}{{29.9}, {29}}},
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
			{0, 12.2, 17.299999999999997, 0, [][]interface{}{{12.2, 15.5, 16.6}, {12, 15, 16}}},
			{1, 17.299999999999997, 22.4, -1, emptyCols},
			{2, 22.4, 27.5, 3, [][]interface{}{{25.5}, {25}}},
			{3, 27.5, 32.6, 4, [][]interface{}{{29.9}, {29}}},
		}

		for i := 0; iter.HasNext(); i++ {
			checkTestWindow(t, iter, expected[i])
		}

		_, w, err := iter.Next()
		assert.Nil(t, w)
		assert.Nil(t, err)
	})

	t.Run("with offset falling after first point", func(t *testing.T) {
		rolling, err := IntervalRolling(b, timeCol, interval, Options{Offset: 2.1})
		assert.Nil(t, err)
		assert.NotNil(t, rolling)
		iter := rolling.(*intervalRollingIterator)

		expected := []testWindow{
			{0, 7.199999999999999, 12.299999999999999, 0, [][]interface{}{{12.2}, {12}}},
			{1, 12.299999999999999, 17.4, 1, [][]interface{}{{15.5, 16.6}, {15, 16}}},
			{2, 17.4, 22.5, -1, emptyCols},
			{3, 22.5, 27.6, 3, [][]interface{}{{25.5}, {25}}},
			{4, 27.6, 32.7, 4, [][]interface{}{{29.9}, {29}}},
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
	// fmt.Println("")
	// fmt.Println("expected", expected.windowIndex, expected.start, expected.end, expected.firstIndex)
	// fmt.Println("actual  ", wi, w.Start, w.End, w.FirstIndex)
	// fmt.Println("expected bow", b)
	// fmt.Println("actual bow  ", w.Bow)
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
