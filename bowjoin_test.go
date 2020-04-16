package bow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOuterJoin(t *testing.T) {
	t.Run("two empty bows", func(t *testing.T) {
		bow1, err := NewBow()
		require.NoError(t, err)
		bow2, err := NewBow()
		require.NoError(t, err)
		expected, err := NewBow()
		require.NoError(t, err)
		bow3 := bow1.OuterJoin(bow2)
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})

	t.Run("empty right bow", func(t *testing.T) {
		bow1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)
		bow2, err := NewBow()
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)
		bow3 := bow1.OuterJoin(bow2)
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})

	t.Run("empty left bow", func(t *testing.T) {
		bow1, err := NewBow()
		require.NoError(t, err)
		bow2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)
		bow3 := bow1.OuterJoin(bow2)
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})

	t.Run("test1", func(t *testing.T) {
		bow1, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{1, 1.1, nil},
				{2, nil, 3},
				{3, 3.3, 4},
				{4, 4.4, 5},
			})
		require.NoError(t, err)
		defer bow1.Release()

		bow2, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col2"},
			[]Type{Int64, Float64, Int64}, [][]interface{}{
				{1, 1.1, 1},
				{2, 0.0, 2},
				{3, 3.0, 3},
				{5, 4.4, 4},
			})
		require.NoError(t, err)
		defer bow2.Release()

		expected, err := NewBowFromRowBasedInterfaces(
			[]string{"index1", "index2", "col1", "col2"},
			[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
				{1, 1.1, 1, 1},
				{1, 1.1, nil, 1},
				{2, nil, 3, nil},
				{3, 3.3, 4, nil},
				{4, 4.4, 5, nil},
				{2, 0.0, nil, 2},
				{3, 3.0, nil, 3},
				{5, 4.4, nil, 4},
			})
		require.NoError(t, err)
		defer expected.Release()

		// fmt.Printf("-%d -%d -%d\n", bow1.NumRows(), bow2.NumRows(), expected.NumRows())
		bow3 := bow1.OuterJoin(bow2)
		defer bow3.Release()
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})

	t.Run("test2", func(t *testing.T) {
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
				{18, 12, -3, 41, 0, nil},
				{42, nil, nil, nil, 4, 42},
			})
		require.NoError(t, err)
		defer expected.Release()

		// fmt.Printf("-%d -%d -%d\n", bow1.NumRows(), bow2.NumRows(), expected.NumRows())
		bow3 := bow1.OuterJoin(bow2)
		defer bow3.Release()
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})

	t.Run("test3", func(t *testing.T) {
		bow1, err := NewBowFromRowBasedInterfaces([]string{"col1", "index1", "col2", "index2"},
			[]Type{Int64, Int64, Int64, Int64}, [][]interface{}{
				{2, 20, 22, 0},
				{5, 4, nil, 2},
				{10, -5, 2, nil},
				{14, nil, 0, 6},
				{14, nil, 0, 6},
				{18, 12, -3, 11},
			})
		require.NoError(t, err)
		defer bow1.Release()

		bow2, err := NewBowFromRowBasedInterfaces([]string{"col3", "index1", "col4", "index2", "col5"},
			[]Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
				{10, -5, 30, nil, 2},
				{10, -5, 30, nil, 2},
				{14, 40, 10, 13, 1},
				{18, 41, 0, nil, -45},
				{42, nil, 4, 6, 0},
				{41, nil, 3, 6, -1},
				{40, nil, 2, 6, -2},
			})
		require.NoError(t, err)
		defer bow2.Release()

		expected, err := NewBowFromRowBasedInterfaces([]string{"col1", "index1", "col2", "index2", "col3", "col4", "col5"},
			[]Type{Int64, Int64, Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
				{2, 20, 22, 0, nil, nil, nil},
				{5, 4, nil, 2, nil, nil, nil},
				{10, -5, 2, nil, 10, 30, 2},
				{10, -5, 2, nil, 10, 30, 2},
				{14, nil, 0, 6, 42, 4, 0},
				{14, nil, 0, 6, 41, 3, -1},
				{14, nil, 0, 6, 40, 2, -2},
				{14, nil, 0, 6, 42, 4, 0},
				{14, nil, 0, 6, 41, 3, -1},
				{14, nil, 0, 6, 40, 2, -2},
				{18, 12, -3, 11, nil, nil, nil},
				{nil, 40, nil, 13, 14, 10, 1},
				{nil, 41, nil, nil, 18, 0, -45},
			})
		require.NoError(t, err)
		defer expected.Release()

		// fmt.Printf("-%d -%d -%d\n", bow1.NumRows(), bow2.NumRows(), expected.NumRows())
		bow3 := bow1.OuterJoin(bow2)
		defer bow3.Release()
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})

	t.Run("test4", func(t *testing.T) {
		bow1, err := NewBowFromRowBasedInterfaces([]string{"index1", "col1", "col2", "index2", "col3"},
			[]Type{Int64, Int64, Int64, Float64, Int64}, [][]interface{}{
				{2, 20, 22, 0.0, 3},
				{nil, nil, nil, nil, nil},
				{5, 4, nil, 2.0, 5},
				{10, -5, 2, nil, 7},
				{2, nil, 0, 0.0, 11},
				{14, nil, 0, 6.0, 0},
				{0, 0, 0, 0.0, 0},
				{18, 4, -3, 2.0, -1},
				{nil, nil, nil, nil, nil},
			})
		require.NoError(t, err)
		defer bow1.Release()

		bow2, err := NewBowFromRowBasedInterfaces([]string{"col4", "index1", "index2", "col5"},
			[]Type{Int64, Int64, Float64, Int64}, [][]interface{}{
				{10, -5, 30.0, nil},
				{10, -3, 1.0, nil},
				{14, 40, 10.0, 13},
				{nil, 41, 0.0, nil},
				{42, -5, 30.0, 6},
				{41, 5, 2.0, 6},
				{nil, nil, nil, nil},
				{40, -5, 30.0, 6},
				{0, 0, 0.0, 0},
			})
		require.NoError(t, err)
		defer bow2.Release()

		expected, err := NewBowFromRowBasedInterfaces([]string{"index1", "col1", "col2", "index2", "col3", "col4", "col5"},
			[]Type{Int64, Int64, Int64, Float64, Int64, Int64, Int64}, [][]interface{}{
				{2, 20, 22, 0.0, 3, nil, nil},
				{nil, nil, nil, nil, nil, nil, nil},
				{5, 4, nil, 2.0, 5, 41, 6},
				{10, -5, 2, nil, 7, nil, nil},
				{2, nil, 0, 0.0, 11, nil, nil},
				{14, nil, 0, 6.0, 0, nil, nil},
				{0, 0, 0, 0.0, 0, 0, 0},
				{18, 4, -3, 2.0, -1, nil, nil},
				{nil, nil, nil, nil, nil, nil, nil},
				{-5, nil, nil, 30.0, nil, 10, nil},
				{-3, nil, nil, 1.0, nil, 10, nil},
				{40, nil, nil, 10.0, nil, 14, 13},
				{41, nil, nil, 0.0, nil, nil, nil},
				{-5, nil, nil, 30.0, nil, 42, 6},
				{-5, nil, nil, 30.0, nil, 40, 6},
			})
		require.NoError(t, err)
		defer expected.Release()

		// fmt.Printf("-%d -%d -%d\n", bow1.NumRows(), bow2.NumRows(), expected.NumRows())
		bow3 := bow1.OuterJoin(bow2)
		defer bow3.Release()
		if !bow3.Equal(expected) {
			t.Error(expected, bow3)
		}
	})
}

func TestInnerJoin(t *testing.T) {
	bow1, err := NewBowFromRowBasedInterfaces(
		[]string{"index1", "index2", "col1"},
		[]Type{Int64, Float64, Int64}, [][]interface{}{
			{1, 1.1, 1},
			{1, 1.1, nil},
			{2, nil, 3},
			{3, 3.3, 4},
			{4, 4.4, 5},
		})
	require.NoError(t, err)
	defer bow1.Release()

	bow2, err := NewBowFromRowBasedInterfaces(
		[]string{"index1", "index2", "col2"},
		[]Type{Int64, Float64, Int64}, [][]interface{}{
			{1, 1.1, 1},
			{2, 0.0, 2},
			{3, 3.0, 3},
			{5, 4.4, 4},
		})
	require.NoError(t, err)
	defer bow2.Release()

	expectedBow, err := NewBowFromRowBasedInterfaces(
		[]string{"index1", "index2", "col1", "col2"},
		[]Type{Int64, Float64, Int64, Int64}, [][]interface{}{
			// 1 is present
			// 1 is present as index 1 and 1.1 are contained twice in bow1 and col1 val is nil
			// 2 is absent as index2 is false on validity bitmap
			// 3 is absent as there is no matching "col1" index
			// 4 is absent as there is no matching "index"
			{1, 1.1, 1, 1},
			{1, 1.1, nil, 1},
		})
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
