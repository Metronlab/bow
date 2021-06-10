package bow

import (
	"fmt"
	"testing"
)

const (
	maxRows     = 1250000
	maxRowsJoin = 5120
	maxCols     = 32
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
	leftBow, err := NewGenBow(
		GenRows(rows),
		GenCols(2),
		GenDataType(typ),
		GenMissingData(true),
		GenRefCol(0, false),
		GenColNames([]string{"A", "B"}))
	if err != nil {
		panic(err)
	}
	rightBow, err := NewGenBow(
		GenRows(rows),
		GenCols(2),
		GenDataType(typ),
		GenMissingData(true),
		GenRefCol(0, false),
		GenColNames([]string{"A", "C"}))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		leftBow.InnerJoin(rightBow)
	}
}

func benchOuterJoin(rows int, typ Type, b *testing.B) {
	leftBow, err := NewGenBow(
		GenRows(rows),
		GenCols(2),
		GenDataType(typ),
		GenMissingData(true),
		GenRefCol(0, false),
		GenColNames([]string{"A", "B"}))
	if err != nil {
		panic(err)
	}
	rightBow, err := NewGenBow(
		GenRows(rows),
		GenCols(2),
		GenDataType(typ),
		GenMissingData(true),
		GenRefCol(0, false),
		GenColNames([]string{"A", "C"}))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		leftBow.OuterJoin(rightBow)
	}
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
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := data.FillPrevious()
		if err != nil {
			panic(err)
		}
	}
}

func benchFillNext(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := data.FillNext()
		if err != nil {
			panic(err)
		}
	}
}

func benchFillMean(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := data.FillMean()
		if err != nil {
			panic(err)
		}
	}
}

func benchFillLinear(rows, cols int, typ Type, b *testing.B) {
	data, err := NewGenBow(
		GenRows(rows),
		GenCols(cols),
		GenDataType(typ),
		GenMissingData(true),
		GenRefCol(0, false))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := data.FillLinear("0", "1")
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkBow_IsColSorted(b *testing.B) {
	for rows := 10; rows <= maxRows; rows *= 50 {
		for typ := Float64; typ <= Int64; typ++ {
			b.Run(fmt.Sprintf("%dx1_%v_Sorted", rows, typ), func(b *testing.B) {
				data, err := NewGenBow(
					GenRows(rows),
					GenCols(1),
					GenDataType(typ),
					GenRefCol(0, false))
				if err != nil {
					panic(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_ = data.IsColSorted(0)
				}
			})
			b.Run(fmt.Sprintf("%dx1_%v_Not_Sorted", rows, typ), func(b *testing.B) {
				data, err := NewGenBow(
					GenRows(rows),
					GenCols(1),
					GenDataType(typ))
				if err != nil {
					panic(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_ = data.IsColSorted(0)
				}
			})
			b.Run(fmt.Sprintf("%dx1_%v_Not_Sorted_With_Missing_Data", rows, typ), func(b *testing.B) {
				data, err := NewGenBow(
					GenRows(rows),
					GenCols(1),
					GenDataType(typ),
					GenMissingData(true))
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
}

func BenchmarkMarshalJSON(b *testing.B) {
	for rows := 10; rows <= maxRows; rows *= 50 {
		b.Run(fmt.Sprintf("%dx4", rows), func(b *testing.B) {
			data, err := NewGenBow(
				GenRows(rows),
				GenCols(4),
				GenDataTypes([]Type{Int64, Float64, String, Bool}),
				GenMissingData(true),
				GenRefCol(0, false),
				GenColNames([]string{"int64", "float64", "bool", "string"}))
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
		})
	}
}

func BenchmarkUnmarshalJSON(b *testing.B) {
	for rows := 10; rows <= maxRows; rows *= 50 {
		b.Run(fmt.Sprintf("%dx4", rows), func(b *testing.B) {
			data, err := NewGenBow(
				GenRows(rows),
				GenCols(4),
				GenDataTypes([]Type{Int64, Float64, String, Bool}),
				GenMissingData(true),
				GenRefCol(0, false),
				GenColNames([]string{"int64", "float64", "bool", "string"}))
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
		})
	}
}
