package bow

import (
	"log"
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

var (
	rows = 10000
	cols = 100
)

func BenchmarkFillNext_Int(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillNext()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillNext_Float(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillNext()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillPrevious_Int(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillPrevious()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillPrevious_Float(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillPrevious()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillMean_Int(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillMean()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillMean_Float(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillMean()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillLinear_Int(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillLinear("3", "6")
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillLinear_Float(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillLinear("3", "6")
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkIsColSorted_Int(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Int64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := newBow.IsColSorted(3)
		if err != nil {
			log.Panicf("IsColSorted error")
		}
	}
}
func BenchmarkIsColSorted_Float(b *testing.B) {
	newBow, err := NewRandomBow(Rows(rows), Cols(cols), DataType(Float64), MissingData(true), RefCol(3))
	if err != nil {
		panic("bow generator error")
	}
	defer newBow.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := newBow.IsColSorted(3)
		if err != nil {
			log.Panicf("IsColSorted error")
		}
	}
}
