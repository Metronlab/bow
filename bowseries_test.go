package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSeriesFromInterfaces(t *testing.T) {
	for _, typ := range allType {
		t.Run(typ.String(), func(t *testing.T) {
			testcase := []interface{}{typ.Convert(0), nil}
			res, err := NewBow(NewSeriesFromInterfaces(typ.String(), typ, testcase))
			require.NoError(t, err)
			fmt.Printf("BOW\n%s\n", res)
			assert.Equal(t, typ.Convert(0), res.GetValue(0, 0))
			assert.Equal(t, nil, res.GetValue(0, 1))
		})
	}
}

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
				NewSeries("test", Int64, dataArray, validArray)
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
