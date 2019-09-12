package aggregation

import (
	"fmt"
	"math/rand"
	"testing"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"git.prod.metronlab.io/backend_libraries/go-bow/bow/rolling"
)

// BenchSize of 1e8 triggers out of memory on a 16Go mem computer
var BenchSize int64

func NoErr(b *testing.B, err error) {
	if err != nil {
		b.Errorf("error: %v", err)
	}
}

func BenchmarkBow(b *testing.B) {
	for _, BenchSize = range []int64{1e3, 1e5, 1e7} {
		b.Run(fmt.Sprintf("Size %d", BenchSize), benchmarkBow)
	}
}

func benchmarkBow(b *testing.B) {
	var benchBow bow.Bow
	var err error

	{
		var Rows [][]interface{}
		b.Run("Creating 'Rows' oriented [][]interface{}", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				Rows = make([][]interface{}, BenchSize)
				rand.Seed(42)
				for i := int64(0); i < BenchSize; i++ {
					Rows[i] = []interface{}{i, rand.Float64()}
				}
			}
		})

		b.Run("rows -> Bow", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				benchBow, err = bow.NewBowFromRowBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					Rows,
				)
			}
		})
		NoErr(b, err)
	}

	{
		var columns [][]interface{}
		b.Run("creating 'Column' oriented [][]interface{}", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				columns = make([][]interface{}, 2)
				rand.Seed(42)
				columns[0] = func(size int64) []interface{} {
					timecol := make([]interface{}, size)
					for i := int64(0); i < size; i++ {
						timecol[i] = i
					}
					return timecol
				}(BenchSize)
				columns[1] = func(size int64) []interface{} {
					valueCol := make([]interface{}, size)
					for i := int64(0); i < size; i++ {
						valueCol[i] = rand.Float64()
					}
					return valueCol
				}(BenchSize)
			}
		})

		b.Run("columns -> bow", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				benchBow, _ = bow.NewBowFromColumnBasedInterfaces(
					[]string{"time", "value"},
					[]bow.Type{bow.Int64, bow.Float64},
					columns,
				)
			}
		})
	}

	{
		var Series []bow.Series
		b.Run("creating 'Series' with validity bitmap", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				Series = make([]bow.Series, 2)
				rand.Seed(42)
				Series[0] = func(size int64) bow.Series {
					tCol := bow.NewSeries(
						"time", bow.Int64,
						make([]int64, size),
						make([]bool, size),
					)
					for i := int64(0); i < size; i++ {
						tCol.Data.Value.([]int64)[i], tCol.Data.Valid[i] = i, true
					}
					return tCol
				}(BenchSize)
				Series[1] = func(size int64) bow.Series {
					tCol := bow.NewSeries(
						"value", bow.Float64,
						make([]float64, size),
						make([]bool, size),
					)
					for i := int64(0); i < size; i++ {
						tCol.Data.Value.([]float64)[i], tCol.Data.Valid[i] = rand.Float64(), true
					}
					return tCol
				}(BenchSize)
			}
		})

		b.Run("Series -> bow", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				benchBow, _ = bow.NewBow(Series...)
			}
		})
	}

	{
		var Series []bow.Series
		b.Run("creating 'Series' with NO validity bitmap", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				Series = make([]bow.Series, 2)
				rand.Seed(42)
				Series[0] = func(size int64) bow.Series {
					tCol := bow.NewSeries(
						"time", bow.Int64,
						make([]int64, size),
						nil,
					)
					for i := int64(0); i < size; i++ {
						tCol.Data.Value.([]int64)[i] = i
					}
					return tCol
				}(BenchSize)
				Series[1] = func(size int64) bow.Series {
					tCol := bow.NewSeries(
						"value", bow.Float64,
						make([]float64, size),
						nil,
					)
					for i := int64(0); i < size; i++ {
						tCol.Data.Value.([]float64)[i] = rand.Float64()
					}
					return tCol
				}(BenchSize)
			}
		})

		b.Run("Series -> bow", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				benchBow, _ = bow.NewBow(Series...)
			}
		})
	}

	{
		var r rolling.Rolling
		b.Run("create rolling", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				r, err = rolling.IntervalRolling(benchBow, "time", 10, rolling.Options{})
				if err != nil {
					panic(err)
				}
			}
		})

		b.Run("aggregate on rolling", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err = r.
					Aggregate(
						WindowStart("time"),
						ArithmeticMean("value")).
					Bow()
				if err != nil {
					panic(err)
				}
			}
		})
	}
}
