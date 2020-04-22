package bow

import "testing"

func BenchmarkIsColSorted(b *testing.B) {
	bowInt, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	bowFloat, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	defer bowInt.Release()
	defer bowFloat.Release()
	b.ResetTimer()

	b.Run("_Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bowInt.IsColSorted(3)
		}
	})
	b.Run("_Float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bowFloat.IsColSorted(3)
		}
	})
}
