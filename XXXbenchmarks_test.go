package bow

import (
	"testing"
)

func BenchmarkBow_InnerJoin(b *testing.B) {
	bow1, err := NewBow(
		NewSeries("index", Int64, []int64{
			1, 2, 3, 4, 5,
			1, 2, 3, 4, 5,
			1, 2, 3, 4, 5,
		}, nil),
		NewSeries("col1", Float64, []float64{
			1.1, 2.2, 3.3, 4, 6,
			1.1, 2.2, 3.3, 4, 6,
			1.1, 2.2, 3.3, 4, 6,
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
			1.1, 2.2, 3.3, 4, 6,
			1.1, 2.2, 3.3, 4, 6,
			1.1, 2.2, 3.3, 4, 6,
		}, nil),
	)
	defer bow2.Release()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		bow3 := bow1.InnerJoin(bow2)
		bow3.Release()
	}
}

var (
	rows = 100000
	cols = 1000
)

func BenchmarkFillPrevious_Int_NoConcurrency(b *testing.B) {
	newBow, err := NewRandomBow(rows, cols, Int64, true)
	if err != nil {
		panic("bow generator error")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillPreviousNoConcurrency()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillPrevious_Int(b *testing.B) {
	newBow, err := NewRandomBow(rows, cols, Int64, true)
	if err != nil {
		panic("bow generator error")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillPrevious()
		if err != nil {
			panic("fill error")
		}
	}

}

func BenchmarkFillPrevious_Float_NoConcurrency(b *testing.B) {
	newBow, err := NewRandomBow(rows, cols, Float64, true)
	if err != nil {
		panic("bow generator error")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillPreviousNoConcurrency()
		if err != nil {
			panic("fill error")
		}
	}
}

func BenchmarkFillPrevious_Float(b *testing.B) {
	newBow, err := NewRandomBow(rows, cols, Float64, true)
	if err != nil {
		panic("bow generator error")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = newBow.FillPrevious()
		if err != nil {
			panic("fill error")
		}
	}
}
