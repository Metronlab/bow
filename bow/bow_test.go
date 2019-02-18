package bow

import (
	"encoding/json"
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

	if err = json.Unmarshal(js, &b2test); err != nil {
		t.Error(err)
	}

	if !b.Equal(b2test) {
		t.Error(b2test, b)
	}
}

func TestBow_InnerJoin(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	defer bow1.Release()
	if err != nil {
		panic(err)
	}

	bow2, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 2, 3, 5}, nil),
		NewSeries("index2", Float64, []float64{1.1, 0, 3, 4.4}, nil),
		NewSeries("col2", Int64, []int64{1, 2, 3, 4}, []bool{true, true, true, true}),
	)
	defer bow2.Release()
	if err != nil {
		panic(err)
	}

	expectedBow, err := NewBow(
		NewSeries("index1", Int64, []int64{
			1, // present
			1, // present as index 1 and 1.1 are contained twice in bow1,
			// 			corresponding col1 val is false in validity bitmap
			// 2 is absent as index2 is false on validity bitmap
			// 3 is absent as there is no matching "col1" index
			// 4 is absent as there is no matching "index"
		}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1}, nil),
		NewSeries("col1", Int64, []int64{1, 1}, []bool{true, false}),
		NewSeries("col2", Int64, []int64{1, 1}, nil),
	)
	defer expectedBow.Release()
	if err != nil {
		panic(err)
	}

	bow3 := bow1.InnerJoin(bow2)
	defer bow3.Release()
	if !bow3.Equal(expectedBow) {
		t.Error(expectedBow, bow3)
	}

	uncomplyingBow, err := NewBow(
		NewSeries("index1", Float64, []float64{1}, nil),
	)
	defer uncomplyingBow.Release()
	if err != nil {
		panic(err)
	}

	defer func() {
		if r := recover(); r == nil ||
			r.(error).Error() != "bow: left and right bow on join columns are of incompatible types: index1" {
			t.Errorf("indexes of bow1 and uncomplyingBow are uncompatible and should panic. Have %v, expect %v",
				r, "bow: left and right bow on join columns are of incompatible types: index1")
		}
	}()
	bow1.InnerJoin(uncomplyingBow)
}
