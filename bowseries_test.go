package bow

import (
	"fmt"
	"testing"
)

func BenchmarkNewSeries(b *testing.B) {
	for rows := 10; rows <= 100000; rows *= 10 {
		dataArray := make([]int64, rows)
		validArray := make([]bool, rows)
		for i := range dataArray {
			dataArray[i] = int64(i)
			validArray[i] = i%2 == 0
		}

		b.Run(fmt.Sprintf("%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				NewSeries("test", dataArray, validArray)
			}
		})
	}
}

func BenchmarkNewSeriesFromInterfaces(b *testing.B) {
	for rows := 10; rows <= 100000; rows *= 10 {
		cells := make([]interface{}, rows)
		for i := range cells {
			cells[i] = int64(i)
		}

		b.Run(fmt.Sprintf("%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				NewSeriesFromInterfaces("test", Int64, cells)
			}
		})
	}
}
