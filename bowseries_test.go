package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkNewSeries(b *testing.B) {
	for rows := 10; rows <= 1000000; rows *= 100 {
		dataArray := make([]int64, rows)
		validArray := make([]bool, rows)
		for i := range dataArray {
			dataArray[i] = int64(i)
			validArray[i] = i%2 == 0
		}
		b.Run(fmt.Sprintf("%d", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				NewSeries("test", Int64, dataArray, validArray)
			}
		})
	}
}

func BenchmarkNewSeriesFromInterfaces(b *testing.B) {
	for rows := 10; rows <= 1000000; rows *= 100 {
		cells := make([]interface{}, rows)
		for i := range cells {
			cells[i] = int64(i)
		}
		b.Run(fmt.Sprintf("%d", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := NewSeriesFromInterfaces("test", Int64, cells)
				require.NoError(b, err)
			}
		})
	}
}
