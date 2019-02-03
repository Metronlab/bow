package bow

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestBow_UnmarshalJSON(t *testing.T) {
	columns := []string{"time", "value", "valueFromJson"}
	ts := make([]Type, len(columns))
	ts[0] = Int64
	rows := [][]interface{}{
		{1, 1.2, json.Number("3")},
		{1, json.Number("1.2"), 3},
		{json.Number("1.1"), 2, 1.3},
	}

	b, err := NewBowFromColumnBasedInterfaces(columns, ts, rows)
	if err != nil {
		t.Error(err)
	}

	b.SetMarshalJSONRowBased(true)
	js, err := b.MarshalJSON()
	if err != nil {
		t.Error(err)
	}

	b2test, err := NewBow()
	if err != nil {
		t.Fatal(err)
	}

	if err = json.Unmarshal(js,&b2test); err != nil {
		t.Error(err)
	}

	if !b.Equal(b2test) {
		fmt.Println("got:")
		b2test.PrintRows()
		fmt.Println("want:")
		b.PrintRows()
		t.Fail()
	}
}

func TestBow_InnerJoin(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index", Int64, []int64{1, 2, 3, 4}, nil),
		NewSeries("col1", Float64, []float64{1.1, 2.2, 3.3, 4}, []bool{true, false, true, true}),
	)
	defer bow1.Release()
	if err != nil {
		panic(err)
	}

	bow2, err := NewBow(
		NewSeries("index", Int64, []int64{1, 2, 3, 5}, nil),
		NewSeries("col2", Int64, []int64{1, 2, 3, 4}, nil),
	)
	defer bow2.Release()
	if err != nil {
		panic(err)
	}

	expectedBow, err := NewBow(
		NewSeries("index", Int64, []int64{1, 2, 3}, nil),
		NewSeries("col1", Float64, []float64{1.1, 2.2, 3.3}, []bool{true, false, true}),
		NewSeries("col2", Int64, []int64{1, 2, 3}, nil),
	)
	defer expectedBow.Release()
	if err != nil {
		panic(err)
	}


	bow3 := bow1.InnerJoin(bow2)
	defer bow3.Release()
	if !bow3.Equal(expectedBow) {
		t.Errorf("expect: \n%v\nhave:%v", expectedBow, bow3)
	}
}

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
	for n:=0; n < b.N; n++ {
		bow3 := bow1.InnerJoin(bow2)
		bow3.Release()
	}
}