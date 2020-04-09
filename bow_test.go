package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestBow_IsColSorted(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		intBobow, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Int64, Int64, Int64, Int64, Int64}, [][]interface{}{
			{-2, 1, nil, nil, -8},
			{0, nil, 3, 4, 0},
			{1, nil, nil, 120, nil},
			{10, 4, 10, 10, -5},
			{13, nil, nil, nil, nil},
			{20, 6, 30, 400, -10},
		})
		sorted, err := intBobow.IsColSorted(0)
		assert.NoError(t, err)
		assert.True(t, sorted)
		sorted, err = intBobow.IsColSorted(1)
		assert.NoError(t, err)
		assert.True(t, sorted)
		sorted, err = intBobow.IsColSorted(2)
		assert.NoError(t, err)
		assert.True(t, sorted)
		sorted, err = intBobow.IsColSorted(3)
		assert.NoError(t, err)
		assert.False(t, sorted)
		sorted, err = intBobow.IsColSorted(4)
		assert.NoError(t, err)
		assert.False(t, sorted)
	})

	t.Run("float64", func(t *testing.T) {
		floatBobow, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{-2.0, 1.0, nil, nil, -8.0},
			{0.0, nil, 3.0, 4.0, 0.0},
			{1.0, nil, nil, 120.0, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{13.0, nil, nil, nil, nil},
			{20.0, 6.0, 30.0, 400.0, -10.0},
		})
		sorted, err := floatBobow.IsColSorted(0)
		assert.NoError(t, err)
		assert.True(t, sorted)
		sorted, err = floatBobow.IsColSorted(1)
		assert.NoError(t, err)
		assert.True(t, sorted)
		sorted, err = floatBobow.IsColSorted(2)
		assert.NoError(t, err)
		assert.True(t, sorted)
		sorted, err = floatBobow.IsColSorted(3)
		assert.NoError(t, err)
		assert.False(t, sorted)
		sorted, err = floatBobow.IsColSorted(4)
		assert.NoError(t, err)
		assert.False(t, sorted)
	})
	t.Run("string (unsupported type)", func(t *testing.T) {
		stringBobow, _ := NewBowFromRowBasedInterfaces([]string{"a", "b"}, []Type{String, String}, [][]interface{}{
			{"egr", "rgr"},
			{"zrr", nil},
			{"zrfr", nil},
			{"rgrg", "zefe"},
			{"zfer", nil},
			{"sffe", "srre"},
		})
		sorted, err := stringBobow.IsColSorted(0)
		assert.Error(t, err)
		assert.False(t, sorted)
		sorted, err = stringBobow.IsColSorted(1)
		assert.Error(t, err)
		assert.False(t, sorted)
	})
}
