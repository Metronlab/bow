package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_SortByCol(t *testing.T) {
	t.Run("sorted", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, 3.1},
			{11, 2.8, 5.9},
			{12, 2.9, 7.5},
			{13, 3.9, 13.4},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, bobow.String(), sorted.String())
	})
	t.Run("simple unsorted", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, 3.1},
			{11, 2.8, 5.9},
			{13, 3.9, 13.4},
			{12, 2.9, 7.5},
		})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, 3.1},
			{11, 2.8, 5.9},
			{12, 2.9, 7.5},
			{13, 3.9, 13.4},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})
	t.Run("simple unsorted different cols", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "time"}, []Type{Float64, Float64, Int64}, [][]interface{}{
			{2.4, 3.1, 10},
			{2.8, 5.9, 11},
			{3.9, 13.4, 13},
			{2.9, 7.5, 12},
		})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"a", "b", "time"}, []Type{Float64, Float64, Int64}, [][]interface{}{
			{2.4, 3.1, 10},
			{2.8, 5.9, 11},
			{2.9, 7.5, 12},
			{3.9, 13.4, 13},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})
	t.Run("simple unsorted with nil values", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, nil},
			{11, 2.8, 5.9},
			{13, nil, 13.4},
			{12, 2.9, 7.5},
		})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, nil},
			{11, 2.8, 5.9},
			{12, 2.9, 7.5},
			{13, nil, 13.4},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})
	t.Run("sorted in desc order", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{13, 3.9, 13.4},
			{12, 2.9, 7.5},
			{11, 2.8, 5.9},
			{10, 2.4, 3.1},
		})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, 3.1},
			{11, 2.8, 5.9},
			{12, 2.9, 7.5},
			{13, 3.9, 13.4},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})
	t.Run("duplicate values - sort by column", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{13, 3.9, 13.4},
			{12, 2.9, 7.5},
			{12, 2.8, 5.9},
			{10, 2.4, 3.1},
		})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{10, 2.4, 3.1},
			{12, 2.9, 7.5},
			{12, 2.8, 5.9},
			{13, 3.9, 13.4},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})
	t.Run("nil values - sort by column", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{13, 3.9, 13.4},
			{12, 2.9, 7.5},
			{nil, 2.8, 5.9},
			{10, 2.4, 3.1},
		})
		require.NoError(t, err)
		expected, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{nil, 2.8, 5.9},
			{10, 2.4, 3.1},
			{12, 2.9, 7.5},
			{13, 3.9, 13.4},
		})
		require.NoError(t, err)
		sorted, err := bobow.SortByCol("time")
		assert.Nil(t, err)
		assert.EqualValues(t, expected.String(), sorted.String())
	})
	t.Run("ERR: empty bow", func(t *testing.T) {
		bobow, err := NewBow()
		require.NoError(t, err)
		_, err = bobow.SortByCol("time")
		assert.Error(t, err)
	})
	t.Run("ERR: missing column", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"other", "a", "b"}, []Type{Int64, Float64, Float64}, [][]interface{}{
			{13, 3.9, 13.4},
			{12, 2.9, 7.5},
			{11, 2.8, 5.9},
			{10, 2.4, 3.1},
		})
		require.NoError(t, err)
		_, err = bobow.SortByCol("time")
		assert.Error(t, err)
	})
	t.Run("ERR: unsupported type - sort by column", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Float64, Float64, Float64}, [][]interface{}{
			{13., 3.9, 13.4},
			{12., 2.9, 7.5},
			{11., 2.8, 5.9},
			{10., 2.4, 3.1},
		})
		require.NoError(t, err)
		_, err = bobow.SortByCol("time")
		assert.Error(t, err)
	})
	t.Run("ERR: unsupported type - other column", func(t *testing.T) {
		bobow, err := NewBowFromRowBasedInterfaces([]string{"time", "a", "b"}, []Type{Int64, Int64, Float64}, [][]interface{}{
			{13, 3, 13.4},
			{12, 2, 7.5},
			{11, 2, 5.9},
			{10, 2, 3.1},
		})
		require.NoError(t, err)
		_, err = bobow.SortByCol("time")
		assert.Error(t, err)
	})
}

func TestBow_Empty(t *testing.T) {
	emptyBow, err := NewBow()
	require.NoError(t, err)

	if emptyBow.NumRows() != 0 || emptyBow.NumCols() != 0 {
		emptyBow.Release()
		t.Errorf("Empty Bow should not have any rows or cols")
	}
}

func TestBow_AppendBows(t *testing.T) {
	colNames := []string{"a", "b"}
	types := []Type{Int64, Float64}

	t.Run("no bow", func(t *testing.T) {
		appended, err := AppendBows()
		assert.Nil(t, err)
		assert.Nil(t, appended)
	})

	t.Run("one empty bow", func(t *testing.T) {
		b, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{{}})
		appended, err := AppendBows(b)
		assert.NoError(t, err)
		assert.True(t, appended.Equal(b), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", b, appended))
	})

	t.Run("first empty bow", func(t *testing.T) {
		b1, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{{}})
		b2, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{{1}})
		appended, err := AppendBows(b1, b2)
		assert.NoError(t, err)
		assert.True(t, appended.Equal(b2), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", b2, appended))
	})

	t.Run("several empty bows", func(t *testing.T) {
		b, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{{}})
		appended, err := AppendBows(b, b)
		assert.NoError(t, err)
		assert.True(t, appended.Equal(b), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", b, appended))
	})

	t.Run("schema type mismatch", func(t *testing.T) {
		b1, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{{1}})
		b2, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Float64}, [][]interface{}{{.2}})
		appended, err := AppendBows(b1, b2)
		assert.EqualError(t, err, "schema mismatch: got both\nschema:\n  fields: 1\n    - a: type=int64\nand\nschema:\n  fields: 1\n    - a: type=float64")
		assert.Nil(t, appended)
	})

	t.Run("schema name mismatch", func(t *testing.T) {
		b1, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{{1}})
		b2, _ := NewBowFromColumnBasedInterfaces([]string{"b"}, []Type{Int64}, [][]interface{}{{2}})
		appended, err := AppendBows(b1, b2)
		assert.EqualError(t, err, "schema mismatch: got both\nschema:\n  fields: 1\n    - a: type=int64\nand\nschema:\n  fields: 1\n    - b: type=int64")
		assert.Nil(t, appended)
	})

	t.Run("3 bows of 2 cols", func(t *testing.T) {
		b1, _ := NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{1, 2, 3}, {.1, .2, .3}})
		b2, _ := NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{4, 5}, {.4, .5}})
		b3, _ := NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{6}, {.6}})
		appended, err := AppendBows(b1, b2, b3)
		expected, _ := NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{
			{1, 2, 3, 4, 5, 6}, {.1, .2, .3, .4, .5, .6}})
		assert.NoError(t, err)
		assert.True(t, appended.Equal(expected), fmt.Sprintf(
			"want:\n%v\nhave:\n%v", expected, appended))
	})
}

func TestBow_NewSlice(t *testing.T) {
	colNames := []string{"time", "value"}
	types := []Type{Int64, Float64}

	origin, err := NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{1, 2, 3}, {.1, .2, .3}})
	assert.NoError(t, err)

	// begin
	expected, err := NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{1}, {.1}})
	assert.NoError(t, err)
	slice := origin.NewSlice(0, 1)
	assert.True(t, expected.Equal(slice), fmt.Sprintf("Have:\n%v,\nExpect:\n%v", expected, slice))

	// end
	expected, err = NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{2, 3}, {.2, .3}})
	assert.NoError(t, err)
	slice = origin.NewSlice(1, 3)
	assert.True(t, expected.Equal(slice), fmt.Sprintf("Have:\n%v,\nExpect:\n%v", expected, slice))

	// empty on already sliced bow (recursive test)
	expected, err = NewBowFromColumnBasedInterfaces(colNames, types, [][]interface{}{{}, {}})
	assert.NoError(t, err)
	slice = slice.NewSlice(1, 1)
	assert.True(t, expected.Equal(slice), fmt.Sprintf("Have:\n%v,\nExpect:\n%v", expected, slice))
}

func TestBow_DropNil(t *testing.T) {
	filledBow, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Int64, Int64}, [][]interface{}{
		{100, 200, 300, 400},
		{110, 220, 330, 440},
		{111, 222, 333, 444},
	})
	holedBow, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Int64, Int64}, [][]interface{}{
		{nil, 200, 300, 400},
		{110, nil, 330, 440},
		{111, nil, 333, nil},
	})

	t.Run("empty bow", func(t *testing.T) {
		b, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		compacted, err := b.DropNil()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("unchanged without nil", func(t *testing.T) {
		compacted, err := filledBow.DropNil()
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(filledBow),
			fmt.Sprintf("want %v\ngot %v", filledBow, compacted))
	})

	t.Run("drop default", func(t *testing.T) {
		compactedDefault, err := holedBow.DropNil()
		assert.Nil(t, err)
		compactedAll, err := holedBow.DropNil("b", "c", "a")
		assert.Nil(t, err)
		assert.True(t, compactedDefault.Equal(compactedAll),
			fmt.Sprintf("default %v\nall %v", compactedDefault, compactedAll))
	})

	t.Run("drop on all columns", func(t *testing.T) {
		compacted, err := holedBow.DropNil()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Int64, Int64}, [][]interface{}{
			{300},
			{330},
			{333},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("drop on one column", func(t *testing.T) {
		compacted, err := holedBow.DropNil("b")
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a", "b", "c"}, []Type{Int64, Int64, Int64}, [][]interface{}{
			{nil, 300, 400},
			{110, 330, 440},
			{111, 333, nil},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})

	t.Run("drop consecutively at start/middle/end", func(t *testing.T) {
		b, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{nil, nil, 1, nil, nil, 2, nil, nil},
		})
		compacted, err := b.DropNil()
		expected, _ := NewBowFromColumnBasedInterfaces([]string{"a"}, []Type{Int64}, [][]interface{}{
			{1, 2},
		})
		assert.Nil(t, err)
		assert.True(t, compacted.Equal(expected),
			fmt.Sprintf("want %v\ngot %v", expected, compacted))
	})
}
