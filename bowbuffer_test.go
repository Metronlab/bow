package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkNewBufferFromInterfaces(b *testing.B) {
	for rows := 10; rows <= 1000000; rows *= 100 {
		cells := make([]interface{}, rows)
		for i := range cells {
			cells[i] = int64(i)
		}
		b.Run(fmt.Sprintf("%d", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := NewBufferFromInterfaces(Int64, cells)
				require.NoError(b, err)
			}
		})
	}
}
