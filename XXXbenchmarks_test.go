package bow

import (
	"fmt"
	"testing"
)

const (
	/*
		maxRows     = 1250000
		maxRowsJoin = 5120
		maxCols     = 32
	*/

	maxRows     = 10
	maxRowsJoin = 10
	maxCols     = 2
)

func BenchmarkJoin(b *testing.B) {
	for rows := 10; rows <= maxRowsJoin; rows *= 2 {
		for typ := Float64; typ <= Int64; typ++ {
			b.Run(fmt.Sprintf("%dx%d_%v_Inner", rows, 2, typ), func(b *testing.B) {
				benchInnerJoin(rows, typ, b)
			})
			b.Run(fmt.Sprintf("%dx%d_%v_Outer", rows, 2, typ), func(b *testing.B) {
				benchOuterJoin(rows, typ, b)
			})
		}
	}
}

func benchInnerJoin(rows int, typ Type, b *testing.B) {
	leftBow, err := NewRandomBow(
		Rows(rows),
		Cols(2),
		DataType(typ),
		MissingData(true),
		RefCol(0, false),
		ColNames([]string{"A", "B"}))
	if err != nil {
		panic(err)
	}
	rightBow, err := NewRandomBow(
		Rows(rows),
		Cols(2),
		DataType(typ),
		MissingData(true),
		RefCol(0, false),
		ColNames([]string{"A", "C"}))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		joined := leftBow.InnerJoin(rightBow)
		joined.Release()
	}
}

func benchOuterJoin(rows int, typ Type, b *testing.B) {
	leftBow, err := NewRandomBow(
		Rows(rows),
		Cols(2),
		DataType(typ),
		MissingData(true),
		RefCol(0, false),
		ColNames([]string{"A", "B"}))
	if err != nil {
		panic(err)
	}
	rightBow, err := NewRandomBow(
		Rows(rows),
		Cols(2),
		DataType(typ),
		MissingData(true),
		RefCol(0, false),
		ColNames([]string{"A", "C"}))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		joined := leftBow.OuterJoin(rightBow)
		joined.Release()
	}
	leftBow.Release()
	rightBow.Release()
}

func BenchmarkBow_Fill(b *testing.B) {
	for cols := 2; cols <= maxCols; cols *= 4 {
		for rows := 10; rows <= maxRows; rows *= 50 {
			for typ := Float64; typ <= Int64; typ++ {
				b.Run(fmt.Sprintf("%dx%d_%v_Previous", rows, cols, typ), func(b *testing.B) {
					benchFillPrevious(rows, cols, typ, b)
				})
				b.Run(fmt.Sprintf("%dx%d_%v_Next", rows, cols, typ), func(b *testing.B) {
					benchFillNext(rows, cols, typ, b)
				})
				b.Run(fmt.Sprintf("%dx%d_%v_Mean", rows, cols, typ), func(b *testing.B) {
					benchFillMean(rows, cols, typ, b)
				})
				b.Run(fmt.Sprintf("%dx%d_%v_Linear", rows, cols, typ), func(b *testing.B) {
					benchFillLinear(rows, cols, typ, b)
				})
			}
		}
	}
}

func benchFillPrevious(rows, cols int, typ Type, b *testing.B) {
	data, err := NewRandomBow(
		Rows(rows),
		Cols(cols),
		DataType(typ),
		MissingData(true))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		filled, err := data.FillPrevious()
		if err != nil {
			panic(err)
		}
		filled.Release()
	}
	data.Release()
}

func benchFillNext(rows, cols int, typ Type, b *testing.B) {
	data, err := NewRandomBow(
		Rows(rows),
		Cols(cols),
		DataType(typ),
		MissingData(true))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		filled, err := data.FillNext()
		if err != nil {
			panic(err)
		}
		filled.Release()
	}
	data.Release()
}

func benchFillMean(rows, cols int, typ Type, b *testing.B) {
	data, err := NewRandomBow(
		Rows(rows),
		Cols(cols),
		DataType(typ),
		MissingData(true))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		filled, err := data.FillMean()
		if err != nil {
			panic(err)
		}
		filled.Release()
	}
	data.Release()
}

func benchFillLinear(rows, cols int, typ Type, b *testing.B) {
	data, err := NewRandomBow(
		Rows(rows),
		Cols(cols),
		DataType(typ),
		MissingData(true),
		RefCol(0, false))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		filled, err := data.FillLinear("0", "1")
		if err != nil {
			panic(err)
		}
		filled.Release()
	}
	data.Release()
}

func BenchmarkBow_IsColSorted(b *testing.B) {
	for rows := 10; rows <= maxRows; rows *= 50 {
		for typ := Float64; typ <= Int64; typ++ {
			b.Run(fmt.Sprintf("%dx1_%v_Sorted", rows, typ), func(b *testing.B) {
				data, err := NewRandomBow(
					Rows(rows),
					Cols(1),
					DataType(typ),
					RefCol(0, false))
				if err != nil {
					panic(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_, _ = data.IsColSorted(0)
				}
				data.Release()
			})
			b.Run(fmt.Sprintf("%dx1_%v_Not_Sorted", rows, typ), func(b *testing.B) {
				data, err := NewRandomBow(
					Rows(rows),
					Cols(1),
					DataType(typ))
				if err != nil {
					panic(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_, _ = data.IsColSorted(0)
				}
				data.Release()
			})
			b.Run(fmt.Sprintf("%dx1_%v_Not_Sorted_With_Missing_Data", rows, typ), func(b *testing.B) {
				data, err := NewRandomBow(
					Rows(rows),
					Cols(1),
					DataType(typ),
					MissingData(true))
				if err != nil {
					panic(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_, _ = data.IsColSorted(0)
				}
				data.Release()
			})
		}
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	for rows := 10; rows <= maxRows; rows *= 50 {
		b.Run(fmt.Sprintf("%dx4", rows), func(b *testing.B) {
			data, err := NewRandomBow(
				Rows(rows),
				Cols(4),
				DataTypes([]Type{Int64, Float64, String, Bool}),
				MissingData(true),
				RefCol(0, false),
				ColNames([]string{"int64", "float64", "bool", "string"}))
			if err != nil {
				panic(err)
			}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_, err := data.MarshalJSON()
				if err != nil {
					panic(err)
				}
			}
			data.Release()
		})
	}
}

func BenchmarkUnmarshalJSON(b *testing.B) {
	for rows := 10; rows <= maxRows; rows *= 50 {
		b.Run(fmt.Sprintf("%dx4", rows), func(b *testing.B) {
			data, err := NewRandomBow(
				Rows(rows),
				Cols(4),
				DataTypes([]Type{Int64, Float64, String, Bool}),
				MissingData(true),
				RefCol(0, false),
				ColNames([]string{"int64", "float64", "bool", "string"}))
			if err != nil {
				panic(err)
			}

			var j []byte

			j, err = data.MarshalJSON()
			if err != nil {
				panic(err)
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				err := NewBowEmpty().UnmarshalJSON(j)
				if err != nil {
					panic(err)
				}
			}
			data.Release()
		})
	}
}
