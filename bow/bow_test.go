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

func TestBow_GetValue(t *testing.T) {
	colNames := []string{"time", "value"}
	types := []Type{Int64, Float64}
	cols := [][]interface{}{
		{1, 2, 3},
		{1.1, 2.2, 3.3},
	}

	b, err := NewBowFromColumnBasedInterfaces(colNames, types, cols)
	if err != nil {
		t.Error(err)
	}

	{
		v := b.GetValue(1, 2)
		expected := 3.3
		if v != expected {
			t.Error(expected, v)
		}
	}
	{
		v := b.GetValueByName("value", 2)
		expected := 3.3
		if v != expected {
			t.Error(expected, v)
		}
	}
	{
		r := b.GetRow(1)
		v := r["value"].(float64)
		expected := 2.2
		if v != expected {
			t.Error(expected, v)
		}
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
		t.Fatal(err)
	}

	bow2, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 2, 3, 5}, nil),
		NewSeries("index2", Float64, []float64{1.1, 0, 3, 4.4}, nil),
		NewSeries("col2", Int64, []int64{1, 2, 3, 4}, []bool{true, true, true, true}),
	)
	defer bow2.Release()
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	bow3 := bow1.InnerJoin(bow2)
	defer bow3.Release()
	if !bow3.Equal(expectedBow) {
		t.Error(expectedBow, bow3)
	}
}

func TestBow_InnerJoin_noResultingRow(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	defer bow1.Release()
	if err != nil {
		t.Fatal(err)
	}

	noResultingValuesBow, err := NewBow(
		NewSeries("index1", Int64, []int64{10}, nil),
		NewSeries("col2", Int64, []int64{10}, nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer noResultingValuesBow.Release()

	expectedBow, err := NewBow(
		NewSeries("index1", Int64, []int64{}, nil),
		NewSeries("index2", Float64, []float64{}, nil),
		NewSeries("col1", Int64, []int64{}, []bool{}),
		NewSeries("col2", Int64, []int64{}, nil),
	)
	defer expectedBow.Release()
	if err != nil {
		t.Fatal(err)
	}

	bow3 := bow1.InnerJoin(noResultingValuesBow)
	if !bow3.Equal(expectedBow) {
		t.Errorf("expect:\n%v\nhave\n%v", expectedBow, bow3)
	}
}

func TestBow_InnerJoin_NonComplyingType(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	defer bow1.Release()
	if err != nil {
		t.Fatal(err)
	}

	uncomplyingBow, err := NewBow(
		NewSeries("index1", Float64, []float64{1}, nil),
	)
	if err != nil {
		panic(err)
	}
	defer uncomplyingBow.Release()
	defer func() {
		if r := recover(); r == nil ||
			r.(error).Error() != "bow: left and right bow on join columns are of incompatible types: index1" {
			t.Errorf("indexes of bow1 and uncomplyingBow are incompatible and should panic. Have %v, expect %v",
				r, "bow: left and right bow on join columns are of incompatible types: index1")
		}
	}()
	bow1.InnerJoin(uncomplyingBow)
}

func TestBow_InnerJoin_NoCommonColumn(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	defer bow1.Release()
	if err != nil {
		t.Fatal(err)
	}

	uncomplyingBow, err := NewBow(
		NewSeries("index3", Float64, []float64{1.1}, nil),
	)
	if err != nil {
		panic(err)
	}

	expectedBow, err := NewBow(
		NewSeries("index1", Int64, []int64{}, nil),
		NewSeries("index2", Float64, []float64{}, nil),
		NewSeries("col1", Int64, []int64{}, nil),
		NewSeries("index3", Float64, []float64{}, []bool{}),
	)
	defer expectedBow.Release()
	if err != nil {
		t.Fatal(err)
	}

	res := bow1.InnerJoin(uncomplyingBow)
	if !res.Equal(expectedBow) {
		t.Errorf("Have:\n%v,\nExpect:\n%v", res, expectedBow)
	}
}

func TestBow_Empty(t *testing.T) {
	emptyBow, err := NewBow()

	if err != nil {
		t.Fatal(err)
	}

	if emptyBow.NumRows() != 0 || emptyBow.NumCols() != 0 {
		emptyBow.Release()
		t.Errorf("Empty Bow should not have any rows or cols")
	}
}
