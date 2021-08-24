package bow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBow_IsColSorted(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		b, _ := NewBowFromRowBasedInterfaces(
			[]string{"a", "b", "c", "d", "e"},
			[]Type{Int64, Int64, Int64, Int64, Int64},
			[][]interface{}{
				{-2, 1, nil, nil, -8},
				{0, nil, 3, 4, 0},
				{1, nil, nil, 120, nil},
				{10, 4, 10, 10, -5},
				{13, nil, nil, nil, nil},
				{20, 6, 30, 400, -10},
			})
		sorted := b.IsColSorted(0)
		assert.True(t, sorted)
		sorted = b.IsColSorted(1)
		assert.True(t, sorted)
		sorted = b.IsColSorted(2)
		assert.True(t, sorted)
		sorted = b.IsColSorted(3)
		assert.False(t, sorted)
		sorted = b.IsColSorted(4)
		assert.False(t, sorted)
	})

	t.Run("float64", func(t *testing.T) {
		b, _ := NewBowFromRowBasedInterfaces([]string{"a", "b", "c", "d", "e"}, []Type{Float64, Float64, Float64, Float64, Float64}, [][]interface{}{
			{-2.0, 1.0, nil, nil, -8.0},
			{0.0, nil, 3.0, 4.0, 0.0},
			{1.0, nil, nil, 120.0, nil},
			{10.0, 4.0, 10.0, 10.0, -5.0},
			{13.0, nil, nil, nil, nil},
			{20.0, 6.0, 30.0, 400.0, -10.0},
		})
		sorted := b.IsColSorted(0)
		assert.True(t, sorted)
		sorted = b.IsColSorted(1)
		assert.True(t, sorted)
		sorted = b.IsColSorted(2)
		assert.True(t, sorted)
		sorted = b.IsColSorted(3)
		assert.False(t, sorted)
		sorted = b.IsColSorted(4)
		assert.False(t, sorted)
	})

	t.Run("string (unsupported type)", func(t *testing.T) {
		b, _ := NewBowFromRowBasedInterfaces([]string{"a", "b"}, []Type{String, String}, [][]interface{}{
			{"egr", "rgr"},
			{"zrr", nil},
			{"zrfr", nil},
			{"rgrg", "zefe"},
			{"zfer", nil},
			{"sffe", "srre"},
		})
		sorted := b.IsColSorted(0)
		assert.False(t, sorted)
		sorted = b.IsColSorted(1)
		assert.False(t, sorted)
	})
}

func BenchmarkBow_IsColSorted(b *testing.B) {
	for rows := 10; rows <= 1000000; rows *= 100 {
		b.Run(fmt.Sprintf("%dx1_%v_Sorted", rows, Float64), func(b *testing.B) {
			data, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(1),
				OptionGenDataType(Float64),
			)
			if err != nil {
				panic(err)
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_ = data.IsColSorted(0)
			}
		})
		b.Run(fmt.Sprintf("%dx1_%v_Not_Sorted", rows, Float64), func(b *testing.B) {
			data, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(1),
				OptionGenDataType(Float64),
				OptionGenType(GenTypeRandom),
			)
			if err != nil {
				panic(err)
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_ = data.IsColSorted(0)
			}
		})
		b.Run(fmt.Sprintf("%dx1_%v_Not_Sorted_With_Missing_Data", rows, Float64), func(b *testing.B) {
			data, err := NewGenBow(
				OptionGenRows(rows),
				OptionGenCols(1),
				OptionGenDataType(Float64),
				OptionGenType(GenTypeRandom),
				OptionGenMissingData(true),
			)
			if err != nil {
				panic(err)
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_ = data.IsColSorted(0)
			}
		})
	}
}

func TestBow_IsColEmpty(t *testing.T) {
	b, err := NewBowFromRowBasedInterfaces(
		[]string{"a", "b", "c"},
		[]Type{Int64, Int64, Int64},
		[][]interface{}{
			{-2, 1, nil},
			{0, nil, nil},
			{1, nil, nil},
		})
	require.NoError(t, err)

	empty := b.IsColEmpty(0)
	assert.False(t, empty)
	empty = b.IsColEmpty(1)
	assert.False(t, empty)
	empty = b.IsColEmpty(2)
	assert.True(t, empty)
}
