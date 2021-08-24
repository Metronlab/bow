package aggregation

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/metronlab/bow"
	"github.com/metronlab/bow/rolling"
	"github.com/stretchr/testify/require"
)

// BenchSize of 1e8 triggers out of memory on a 16Go mem computer
var BenchSize int64

func BenchmarkBow(b *testing.B) {
	for _, BenchSize = range []int64{1, 1e3, 1e5, 1e7} {
		b.Run(fmt.Sprintf("Size %d", BenchSize), benchmarkBow)
	}
}

func benchmarkBow(b *testing.B) {
	var benchBow bow.Bow
	var err error

	rows := make([][]interface{}, BenchSize)
	rand.Seed(42)
	for i := int64(0); i < BenchSize; i++ {
		rows[i] = []interface{}{i, rand.Float64()}
	}

	b.Run("NewBowFromRowBasedInterfaces", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchBow, err = bow.NewBowFromRowBasedInterfaces(
				[]string{"time", "value"},
				[]bow.Type{bow.Int64, bow.Float64},
				rows,
			)
			require.NoError(b, err)
		}
	})

	columns := make([][]interface{}, 2)
	rand.Seed(42)
	columns[0] = func(size int64) []interface{} {
		timeCol := make([]interface{}, size)
		for i := int64(0); i < size; i++ {
			timeCol[i] = i
		}
		return timeCol
	}(BenchSize)
	columns[1] = func(size int64) []interface{} {
		valueCol := make([]interface{}, size)
		for i := int64(0); i < size; i++ {
			valueCol[i] = rand.Float64()
		}
		return valueCol
	}(BenchSize)

	b.Run("NewBowFromColBasedInterfaces", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchBow, err = bow.NewBowFromColBasedInterfaces(
				[]string{"time", "value"},
				[]bow.Type{bow.Int64, bow.Float64},
				columns,
			)
			require.NoError(b, err)
		}
	})

	seriesSlice := make([]bow.Series, 2)
	rand.Seed(42)
	seriesSlice[0] = func(size int64) bow.Series {
		buf := bow.NewBuffer(int(size), bow.Int64)
		for i := int64(0); i < size; i++ {
			buf.SetOrDrop(int(i), i)
		}
		return bow.NewSeriesFromBuffer("time", buf)
	}(BenchSize)
	seriesSlice[1] = func(size int64) bow.Series {
		buf := bow.NewBuffer(int(size), bow.Float64)
		for i := int64(0); i < size; i++ {
			buf.SetOrDrop(int(i), rand.Float64())
		}
		return bow.NewSeriesFromBuffer("value", buf)
	}(BenchSize)

	b.Run("NewBow with validity bitmap", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchBow, err = bow.NewBow(seriesSlice...)
			require.NoError(b, err)
		}
	})

	seriesSlice = make([]bow.Series, 2)
	rand.Seed(42)
	seriesSlice[0] = func(size int64) bow.Series {
		buf := bow.NewBuffer(int(size), bow.Int64)
		for i := int64(0); i < size; i++ {
			buf.Data.([]int64)[i] = i
		}
		return bow.NewSeries("time", buf.Data, nil)
	}(BenchSize)
	seriesSlice[1] = func(size int64) bow.Series {
		buf := bow.NewBuffer(int(size), bow.Float64)
		for i := int64(0); i < size; i++ {
			buf.Data.([]float64)[i] = rand.Float64()
		}
		return bow.NewSeries("value", buf.Data, nil)
	}(BenchSize)

	b.Run("NewBow without validity bitmap", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchBow, err = bow.NewBow(seriesSlice...)
			require.NoError(b, err)
		}
	})

	var r rolling.Rolling
	b.Run("rolling.IntervalRolling", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			r, err = rolling.IntervalRolling(benchBow, "time", 10, rolling.Options{})
			require.NoError(b, err)
		}
	})

	b.Run("rolling.Rolling.Aggregate", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err = r.Aggregate(WindowStart("time"), ArithmeticMean("value")).Bow()
			require.NoError(b, err)
		}
	})
}
