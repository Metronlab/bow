package bow

import (
	"testing"
)

func BenchmarkJoin(b *testing.B) {
	bow1, err := NewBow(
		NewSeries("index", Int64, []int64{
			1, 2, 3, 4, 5,
			1, 2, 3, 4, 5,
			1, 2, 3, 4, 5,
		}, nil),
		NewSeries("col1", Float64, []float64{
			1.1, 2.2, 3.3, 4., 6.,
			1.1, 2.2, 3.3, 4., 6.,
			1.1, 2.2, 3.3, 4., 6.,
		}, nil),
	)
	defer bow1.Release()
	if err != nil {
		panic(err)
	}

	bow2, err := NewBow(
		NewSeries("index", Int64, []int64{
			1, 2, 3, 4, 5,
			1, 2, 3, 4, 5,
			1, 2, 3, 4, 5,
		}, nil),
		NewSeries("col2", Float64, []float64{
			1.1, 2.2, 3.3, 4., 6.,
			1.1, 2.2, 3.3, 4., 6.,
			1.1, 2.2, 3.3, 4., 6.,
		}, nil),
	)
	defer bow2.Release()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.Run("Inner", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			bow3 := bow1.InnerJoin(bow2)
			bow3.Release()
		}
	})
	b.Run("Inner2", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			bow3 := bow1.InnerJoin2(bow2)
			bow3.Release()
		}
	})
	b.Run("Outer", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			bow3 := bow1.OuterJoin(bow2)
			bow3.Release()
		}
	})
}

const (
	rows = 10000
	cols = 100
)

func BenchmarkFill(b *testing.B) {
	bowInt, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	bowFloat, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	bowIntRef, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	bowFloatRef, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	defer bowInt.Release()
	defer bowFloat.Release()
	defer bowIntRef.Release()
	defer bowFloatRef.Release()

	b.ResetTimer()

	b.Run("Next_Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowInt.FillNext()
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Next_Float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowFloat.FillNext()
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Previous_Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowInt.FillPrevious()
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Previous_Float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowFloat.FillPrevious()
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Mean_Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowInt.FillMean()
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Mean_Float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowFloat.FillMean()
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Linear_Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowIntRef.FillLinear("3", "6")
			if err != nil {
				panic("fill error")
			}
		}
	})
	b.Run("Linear_Float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = bowFloatRef.FillLinear("3", "6")
			if err != nil {
				panic("fill error")
			}
		}
	})
}
