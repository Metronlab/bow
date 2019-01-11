package bow

func ExampleNewBow() {
	bow, err := NewBow(
		NewSeries("col1", Int64, []int64{1, 2, 3, 4}, nil),
		NewSeries("col2", Float64, []float64{1.1, 2.2, 3.3, 4}, []bool{true, false, true, true}),
		NewSeries("col3", Bool, []bool{true, false, true, false}, []bool{true, false, true, true}),
	)
	if err != nil {
		panic(err)
	}

	bow.PrintRows()
	// output:
	// col1  col2   col3
	// 1     1.1    true
	// 2     <nil>  <nil>
	// 3     3.3    true
	// 4     4      false
}
