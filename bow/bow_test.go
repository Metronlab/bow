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
		NewSeries("index", Int64, []int64{1, 2, 3, 4}, nil),
		NewSeries("col1", Float64, []float64{1.1, 2.2, 3.3, 4.4}, []bool{true, false, true, true}),
	)
	defer bow1.Release()
	if err != nil {
		panic(err)
	}

	bow2, err := NewBow(
		NewSeries("index", Int64, []int64{1, 2, 3, 5}, nil),
		NewSeries("col1", Float64, []float64{1.1, 0, 3, 4.4}, []bool{true, false, true, true}),
		NewSeries("col2", Int64, []int64{1, 2, 3, 4}, nil),
	)
	defer bow2.Release()
	if err != nil {
		panic(err)
	}

	expectedBow, err := NewBow(
		NewSeries("index", Int64, []int64{
			1, // present
			2, // present with col2 value as false on validity bitmap
			// 3 is absent as there is no matching "col1" index
			// 4 is absent as there is no matching "index"
		}, nil),
		NewSeries("col1", Float64, []float64{1.1, 2.2}, []bool{true, false}),
		NewSeries("col2", Int64, []int64{1, 2}, nil),
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
		NewSeries("index", Float64, []float64{1, 2, 3, 4}, nil),
		NewSeries("col1", Float64, []float64{1.1, 2.2, 3.3, 4.4}, []bool{true, false, true, true}),
	)
	defer uncomplyingBow.Release()
	if err != nil {
		panic(err)
	}

	defer func() {
		if r := recover(); r == nil ||
			r.(error).Error() != "bow: left and right bow on join columns are of incompatible types: index" {
			t.Errorf("indexes of bow1 and uncomplyingBow are uncompatible and should panic. Have %v, expect %v",
				r, "bow: left and right bow on join columns are of incompatible types: index")
		}
	}()
	bow1.InnerJoin(uncomplyingBow)
}
