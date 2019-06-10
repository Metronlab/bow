package transformation

import (
	"fmt"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/stretchr/testify/assert"
)

var (
	timeCol   = 0
	valueCol  = 1
	badCol    = 99
	emptyCols = [][]interface{}{{}, {}}
)

func TestArithmeticMean_Init(t *testing.T) {
	{
		aggr := NewArithmeticMean(badCol, valueCol)
		wa, err := aggr.Apply(bow.Window{Bow: newAggregationTestBow([][]interface{}{{1}, {1.1}})})
		assert.EqualError(t, err, fmt.Sprintf("no index column %d", badCol))
		assert.Nil(t, wa)
	}
	{
		aggr := NewArithmeticMean(timeCol, badCol)
		wa, err := aggr.Apply(bow.Window{Bow: newAggregationTestBow([][]interface{}{{1}, {1.1}})})
		assert.EqualError(t, err, fmt.Sprintf("no value column %d", badCol))
		assert.Nil(t, wa)
	}
	{ // an empty bow gives an empty window
		aggr := NewArithmeticMean(timeCol, valueCol)
		wa, err := aggr.Apply(bow.Window{Bow: newAggregationTestBow(emptyCols)})
		assert.Nil(t, err)
		assert.NotNil(t, wa)
	}
}

func TestArithmeticMean_Next(t *testing.T) {
	var interval int64 = 3
	b := newAggregationTestBow([][]interface{}{
		{
			0, 1, 2,
			3, 4, 5,
			6, 7, 8,
			//
			12},
		{
			5.0, 5.0, 5.0,
			6.0, 7.0, 8.0,
			0.0, 0.0, 100.0,
			//
			100.0}})

	aggr := NewArithmeticMean(timeCol, valueCol)
	iter, err := b.IntervalRolling(timeCol, interval, bow.IntervalRollingOptions{})
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	fmt.Println("full bow")
	fmt.Println(b)

	next(t, 0, 2,
		iter, [][]interface{}{{0, 1, 2}, {5.0, 5.0, 5.0}},
		aggr, [][]interface{}{{0}, {5.0}})

	next(t, 3, 5,
		iter, [][]interface{}{{3, 4, 5}, {6.0, 7.0, 8.0}},
		aggr, [][]interface{}{{3}, {7.0}})

	next(t, 6, 8,
		iter, [][]interface{}{{6, 7, 8}, {0.0, 0.0, 100.0}},
		aggr, [][]interface{}{{6}, {33.333333333333336}}) // todo: precision

	next(t, 9, 11,
		iter, emptyCols,
		aggr, emptyCols)

	next(t, 12, 14,
		iter, [][]interface{}{{12}, {100.0}},
		aggr, [][]interface{}{{12}, {100.0}})

	w, err := iter.Next()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

func next(t *testing.T,
	start, end int64,
	iter bow.RollingIterator,
	iterColumns [][]interface{},
	aggr ArithmeticMean,
	aggrColumns [][]interface{}) {
	w, err := iter.Next()
	assert.NotNil(t, w)
	assert.Nil(t, err)
	fmt.Println("iter window", w.Start, w.End)
	fmt.Println(w.Bow)
	assert.Equal(t, int64(start), w.Start)
	assert.Equal(t, int64(end), w.End)
	assert.Equal(t, true, w.Bow.Equal(
		newAggregationTestBow(iterColumns)))
	// aggr
	wa, err := aggr.Apply(*w)
	assert.NotNil(t, wa)
	assert.Nil(t, err)
	fmt.Println("aggr window", wa.Start, wa.End)
	fmt.Println(wa.Bow)
	assert.Equal(t, int64(start), wa.Start)
	assert.Equal(t, int64(end), wa.End)
	assert.Equal(t, true, wa.Bow.Equal(
		newAggregationTestBow(aggrColumns)))
}

func newAggregationTestBow(series [][]interface{}) bow.Bow {
	colNames := make([]string, 2)
	colNames[timeCol] = "time"
	colNames[valueCol] = "value"
	types := []bow.Type{bow.Int64, bow.Float64}
	b, err := bow.NewBowFromColumnBasedInterfaces(colNames, types, series)
	if err != nil {
		panic(err)
	}
	return b
}
