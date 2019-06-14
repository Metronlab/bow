package bow

import "testing"

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
