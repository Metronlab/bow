package bow

func ExampleNewBow() {
	bow, err := NewBow(
		NewSeries("col1", Int64, []int64{1,2,3,4}, nil),
		NewSeries("col2", Float64, []float64{1.1,2.2,3.3,4}, []bool{true, false, true, true}),
		NewSeries("col3", Bool, []bool{true, false, true, false}, []bool{true, false, true, true}),
	)
	if err != nil {
		panic(err)
	}

	bow.PrintRows()
	// output:
	//col1: 1, col2: 1.1
	//col1: 2, col2: <nil>
	//col1: 3, col2: 3.3
	//col1: 4, col2: 4
}
