package bow

import (
	"fmt"
	"testing"
)

func BenchmarkNewBufferFromInterfaces(b *testing.B) {
	for rows := 10; rows <= 1000000; rows *= 100 {
		b.Run(fmt.Sprintf("%d_%v", rows, Float64), func(b *testing.B) {
			cells := make([]interface{}, rows)
			for i := range cells {
				cells[i] = i
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_, err := NewBufferFromInterfaces(Float64, cells)
				if err != nil {
					panic(err)
				}
			}
		})
	}
}
