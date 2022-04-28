package rolling

import (
	"fmt"
	"testing"

	"github.com/metronlab/bow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	timeCol  = "time"
	valueCol = "value"
	badCol   = "badcol"

	emptyCols = [][]interface{}{{}, {}}
)

func TestIntervalRolling_NumWindows(t *testing.T) {
	t.Run("empty bow", func(t *testing.T) {
		r, err := IntervalRolling(newIntervalRollingTestBow(t, emptyCols), timeCol, 1, Options{})
		require.NoError(t, err)
		n, err := r.NumWindows()
		assert.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("one liner bow", func(t *testing.T) {
		r, err := IntervalRolling(newIntervalRollingTestBow(t, [][]interface{}{
			{0}, {1.},
		}), timeCol, 1, Options{})
		require.NoError(t, err)
		n, err := r.NumWindows()
		assert.Nil(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("points in same window", func(t *testing.T) {
		r, err := IntervalRolling(newIntervalRollingTestBow(t, [][]interface{}{
			{0, 9}, {1., 1.},
		}), timeCol, 10, Options{})
		require.NoError(t, err)
		n, err := r.NumWindows()
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("excluded point goes in next window", func(t *testing.T) {
		r, err := IntervalRolling(newIntervalRollingTestBow(t, [][]interface{}{
			{0, 10}, {1., 1.},
		}), timeCol, 10, Options{})
		require.NoError(t, err)
		n, err := r.NumWindows()
		assert.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("offset puts first value in preceding window", func(t *testing.T) {
		r, err := IntervalRolling(newIntervalRollingTestBow(t, [][]interface{}{
			{0, 9}, {1., 1.},
		}), timeCol, 10, Options{Offset: 1})
		require.NoError(t, err)
		n, err := r.NumWindows()
		assert.NoError(t, err)
		assert.Equal(t, 2, n)
	})
}

func TestIntervalRolling_iterator_init(t *testing.T) {
	t.Run("interval < 0", func(t *testing.T) {
		b := newIntervalRollingTestBow(t, [][]interface{}{{0}, {1.}})
		rolling, err := IntervalRolling(b, timeCol, 0, Options{})
		assert.EqualError(t, err, "enforceIntervalAndOffset: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("interval == 0", func(t *testing.T) {
		b := newIntervalRollingTestBow(t, [][]interface{}{{0}, {1.}})
		rolling, err := IntervalRolling(b, timeCol, 0, Options{})
		assert.EqualError(t, err, "enforceIntervalAndOffset: strictly positive interval required")
		assert.Nil(t, rolling)
	})

	t.Run("non existing index", func(t *testing.T) {
		b := newIntervalRollingTestBow(t, [][]interface{}{{0}, {1.}})
		_, err := IntervalRolling(b, badCol, 1, Options{})
		assert.EqualError(t, err, fmt.Sprintf("no column '%s'", badCol))
	})

	t.Run("invalid interval type", func(t *testing.T) {
		b, _ := bow.NewBowFromColBasedInterfaces(
			[]string{timeCol},
			[]bow.Type{bow.Float64},
			[][]interface{}{{0.}})
		_, err := IntervalRolling(b, timeCol, 1, Options{})
		assert.EqualError(t, err, "impossible to create a new intervalRolling on column of type float64")
	})

	t.Run("empty bow gives valid finished iterator", func(t *testing.T) {
		b := newIntervalRollingTestBow(t, emptyCols)
		r, err := IntervalRolling(b, timeCol, 1, Options{})
		assert.NoError(t, err)
		rCopy := r.(*intervalRolling)
		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})
}

func TestIntervalRolling_iterate(t *testing.T) {
	var interval int64 = 5
	b := newIntervalRollingTestBow(t,
		[][]interface{}{
			{12, 15, 16, 25, 25, 29}, // 25 is a duplicated index on ref column
			{1.2, 1.5, 1.6, 2.5, 3.5, 2.9},
		})

	t.Run("no option", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 10, 15, 0, [][]interface{}{{12}, {1.2}}},
			{1, 15, 20, 1, [][]interface{}{{15, 16}, {1.5, 1.6}}},
			{2, 20, 25, 3, emptyCols},
			{3, 25, 30, 3, [][]interface{}{{25, 25, 29}, {2.5, 3.5, 2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("with inclusive windows", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Inclusive: true})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 10, 15, 0, [][]interface{}{{12, 15}, {1.2, 1.5}}},
			{1, 15, 20, 1, [][]interface{}{{15, 16}, {1.5, 1.6}}},
			{2, 20, 25, 3, [][]interface{}{{25}, {2.5}}},
			{3, 25, 30, 3, [][]interface{}{{25, 25, 29}, {2.5, 3.5, 2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("with offset falling before first point", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Offset: 1})
		assert.Nil(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 11, 16, 0, [][]interface{}{{12, 15}, {1.2, 1.5}}},
			{1, 16, 21, 2, [][]interface{}{{16}, {1.6}}},
			{2, 21, 26, 3, [][]interface{}{{25, 25}, {2.5, 3.5}}},
			{3, 26, 31, 5, [][]interface{}{{29}, {2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("with offset falling at first point", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Offset: 2})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 12, 17, 0, [][]interface{}{{12, 15, 16}, {1.2, 1.5, 1.6}}},
			{1, 17, 22, 3, emptyCols},
			{2, 22, 27, 3, [][]interface{}{{25, 25}, {2.5, 3.5}}},
			{3, 27, 32, 5, [][]interface{}{{29}, {2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("with offset falling after first point", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Offset: 3})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 8, 13, 0, [][]interface{}{{12}, {1.2}}},
			{1, 13, 18, 1, [][]interface{}{{15, 16}, {1.5, 1.6}}},
			{2, 18, 23, 3, emptyCols},
			{3, 23, 28, 3, [][]interface{}{{25, 25}, {2.5, 3.5}}},
			{4, 28, 33, 5, [][]interface{}{{29}, {2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("offset > interval", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Offset: 8})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 8, 13, 0, [][]interface{}{{12}, {1.2}}},
			{1, 13, 18, 1, [][]interface{}{{15, 16}, {1.5, 1.6}}},
			{2, 18, 23, 3, emptyCols},
			{3, 23, 28, 3, [][]interface{}{{25, 25}, {2.5, 3.5}}},
			{4, 28, 33, 5, [][]interface{}{{29}, {2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("offset == interval", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Offset: 5})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 10, 15, 0, [][]interface{}{{12}, {1.2}}},
			{1, 15, 20, 1, [][]interface{}{{15, 16}, {1.5, 1.6}}},
			{2, 20, 25, 3, emptyCols},
			{3, 25, 30, 3, [][]interface{}{{25, 25, 29}, {2.5, 3.5, 2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})

	t.Run("offset < 0", func(t *testing.T) {
		r, err := IntervalRolling(b, timeCol, interval, Options{Offset: -2})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rCopy := r.(*intervalRolling)

		expected := []testWindow{
			{0, 8, 13, 0, [][]interface{}{{12}, {1.2}}},
			{1, 13, 18, 1, [][]interface{}{{15, 16}, {1.5, 1.6}}},
			{2, 18, 23, 3, emptyCols},
			{3, 23, 28, 3, [][]interface{}{{25, 25}, {2.5, 3.5}}},
			{4, 28, 33, 5, [][]interface{}{{29}, {2.9}}},
		}

		for i := 0; rCopy.HasNext(); i++ {
			checkTestWindow(t, rCopy, expected[i])
		}

		_, w, err := rCopy.Next()
		assert.Nil(t, w)
		assert.NoError(t, err)
	})
}

type testWindow struct {
	windowIndex int
	start       int64
	end         int64
	firstIndex  int
	cols        [][]interface{}
}

func checkTestWindow(t *testing.T, r *intervalRolling, expected testWindow) {
	wi, w, err := r.Next()
	assert.Equal(t, expected.windowIndex, wi)
	assert.NotNil(t, w)
	assert.NoError(t, err)

	assert.Equal(t, expected.start, w.FirstValue)
	assert.Equal(t, expected.end, w.LastValue)
	assert.Equal(t, expected.firstIndex, w.FirstIndex)
	b := newIntervalRollingTestBow(t, expected.cols)
	assert.True(t, w.Bow.Equal(b), "expect: %v\nhave: %v", b, w.Bow)
}

func newIntervalRollingTestBow(t *testing.T, cols [][]interface{}) bow.Bow {
	colNames := []string{timeCol, valueCol}
	colTypes := []bow.Type{bow.Int64, bow.Float64}
	b, err := bow.NewBowFromColBasedInterfaces(colNames, colTypes, cols)
	require.NoError(t, err)
	return b
}
