package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOuterJoin(t *testing.T) {
	bow1, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c"},
		[]Type{Int64, Int64, Int64}, [][]interface{}{
			{2, 20, 22},
			{5, 4, nil},
			{10, -5, 2},
			{14, nil, 0},
			{18, 12, -3},
		})
	require.NoError(t, err)
	defer bow1.Release()

	bow2, err := NewBowFromRowBasedInterfaces([]string{"a", "d", "e", "f"},
		[]Type{Int64, Int64, Int64, Int64}, [][]interface{}{
			{10, nil, 30, 5},
			{14, 40, 10, 13},
			{18, 41, 0, nil},
			{42, nil, 4, 42},
		})
	require.NoError(t, err)
	defer bow2.Release()

	expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e", "f"},
		[]Type{Int64, Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{2, 20, 22, nil, nil, nil},
			{5, 4, nil, nil, nil, nil},
			{10, -5, 2, nil, 30, 5},
			{14, nil, 0, 40, 10, 13},
			{18, 12, -3, nil, 4, 42},
		})
	require.NoError(t, err)
	defer expected.Release()

	bow3 := bow1.OuterJoin(bow2, "a")
	defer bow3.Release()
	fmt.Print(bow3.String())
	//if !bow3.Equal(expected) {
	//	t.Error(expected, bow3)
	//}
}

func TestInnerJoin(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	require.NoError(t, err)
	defer bow1.Release()

	bow2, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 2, 3, 5}, nil),
		NewSeries("index2", Float64, []float64{1.1, 0, 3, 4.4}, nil),
		NewSeries("col2", Int64, []int64{1, 2, 3, 4}, []bool{true, true, true, true}),
	)
	require.NoError(t, err)
	defer bow2.Release()

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
	require.NoError(t, err)
	defer expectedBow.Release()

	bow3 := bow1.InnerJoin(bow2)
	defer bow3.Release()
	if !bow3.Equal(expectedBow) {
		t.Error(expectedBow, bow3)
	}
}

func TestInnerJoin_noResultingRow(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	require.NoError(t, err)
	defer bow1.Release()

	noResultingValuesBow, err := NewBow(
		NewSeries("index1", Int64, []int64{10}, nil),
		NewSeries("col2", Int64, []int64{10}, nil),
	)
	require.NoError(t, err)
	defer noResultingValuesBow.Release()

	expectedBow, err := NewBow(
		NewSeries("index1", Int64, []int64{}, nil),
		NewSeries("index2", Float64, []float64{}, nil),
		NewSeries("col1", Int64, []int64{}, []bool{}),
		NewSeries("col2", Int64, []int64{}, nil),
	)
	require.NoError(t, err)
	defer expectedBow.Release()

	bow3 := bow1.InnerJoin(noResultingValuesBow)
	if !bow3.Equal(expectedBow) {
		t.Errorf("expect:\n%v\nhave\n%v", expectedBow, bow3)
	}
}

func TestInnerJoin_NonComplyingType(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	require.NoError(t, err)
	defer bow1.Release()

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

func TestInnerJoin_NoCommonColumn(t *testing.T) {
	bow1, err := NewBow(
		NewSeries("index1", Int64, []int64{1, 1, 2, 3, 4}, nil),
		NewSeries("index2", Float64, []float64{1.1, 1.1, 2.2, 3.3, 4.4}, []bool{true, true, false, true, true}),
		NewSeries("col1", Int64, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}),
	)
	require.NoError(t, err)
	defer bow1.Release()

	uncomplyingBow, err := NewBow(
		NewSeries("index3", Float64, []float64{1.1}, nil),
	)
	require.NoError(t, err)

	expectedBow, err := NewBow(
		NewSeries("index1", Int64, []int64{}, nil),
		NewSeries("index2", Float64, []float64{}, nil),
		NewSeries("col1", Int64, []int64{}, nil),
		NewSeries("index3", Float64, []float64{}, []bool{}),
	)
	require.NoError(t, err)
	defer expectedBow.Release()

	res := bow1.InnerJoin(uncomplyingBow)
	if !res.Equal(expectedBow) {
		t.Errorf("Have:\n%v,\nExpect:\n%v", res, expectedBow)
	}
}
