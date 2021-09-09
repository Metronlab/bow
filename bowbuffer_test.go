package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkNewBufferFromInterfaces(b *testing.B) {
	for rows := 10; rows <= 100000; rows *= 10 {
		cells := make([]interface{}, rows)
		for i := range cells {
			cells[i] = int64(i)
		}

		b.Run(fmt.Sprintf("%d_rows", rows), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := NewBufferFromInterfaces(Int64, cells)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkBuffer_SetOrDrop(b *testing.B) {
	buf := NewBuffer(10, Int64)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf.SetOrDrop(9, int64(3))
		buf.SetOrDrop(9, nil)
	}
}

func BenchmarkBuffer_SetOrStrict(b *testing.B) {
	buf := NewBuffer(10, Int64)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf.SetOrDropStrict(9, int64(3))
		buf.SetOrDropStrict(9, nil)
	}
}

func BenchmarkBuffer_GetValue(b *testing.B) {
	buf := NewBuffer(10, Int64)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = buf.GetValue(9)
	}
}
