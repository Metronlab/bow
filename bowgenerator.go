package bow

import (
	"fmt"
	"math/rand"
	"strconv"
)

type randomBowOptions struct {
	Rows        int
	Cols        int
	DataType    Type
	MissingData bool
	RefCol      int
	DescSort    bool
}

// Option is a type used for self-referential functions
type Option func(*randomBowOptions)

// Rows defines the number of rows for NewRandomBow
func Rows(rows int) Option {
	return func(f *randomBowOptions) {
		if rows < 1 {
			panic("NewRandomBow: Rows value must be positive")
		}
		f.Rows = rows
	}
}

// Cols defines the number of columns for NewRandomBow
func Cols(cols int) Option {
	return func(f *randomBowOptions) {
		if cols < 1 {
			panic("NewRandomBow: Cols value must be positive")
		}
		f.Cols = cols
	}
}

// DataType defines the data type for NewRandomBow
func DataType(typ Type) Option { return func(f *randomBowOptions) { f.DataType = typ } }

// MissingData defines if the NewRandomBow includes random missing data
func MissingData(missing bool) Option { return func(f *randomBowOptions) { f.MissingData = missing } }

// RefCol defines the index of a reference column, which does not include missing data and is sorted
func RefCol(refCol int) Option { return func(f *randomBowOptions) { f.RefCol = refCol } }

// DescSort defines the number of rows for NewRandomBow
func DescSort(descSort bool) Option { return func(f *randomBowOptions) { f.DescSort = descSort } }

// NewRandomBow generates a new random bow filled with the following options:
// Rows(rows int): number of rows (default 10)
// Cols(cols int): number of columns (default 10)
// DataType(typ Type): data type (default Int64)
// MissingData(missing bool): enable random missing data (default false)
// RefCol(refCol int): defines the index of a reference column, which does not include missing data and is sorted (default -1 = no column)
// DescSort(descSort bool): column index sorted in descending order (default false)
func NewRandomBow(options ...Option) (Bow, error) {
	// Set default options
	f := &randomBowOptions{
		Rows:     10,
		Cols:     10,
		DataType: Int64,
		RefCol:   -1,
	}
	for _, option := range options {
		option(f)
	}
	if f.DataType != Int64 && f.DataType != Float64 {
		err := fmt.Errorf("random bow generation error: data type must be Int64 or Float64")
		return nil, err
	}
	series := make([]Series, f.Cols)
	for i := range series {
		if i == f.RefCol {
			series[i] = newSortedRandomSeries(strconv.Itoa(i), f.DataType, f.Rows, f.DescSort)
		} else {
			series[i] = newRandomSeries(strconv.Itoa(i), f.DataType, f.Rows)
		}
	}
	if f.MissingData {
		for sIndex, s := range series {
			if sIndex != f.RefCol {
				nils := rand.Intn(int(f.Rows))
				for j := 0; j < nils; j++ {
					nils2 := rand.Intn(int(f.Rows))
					s.Data.SetOrDrop(nils2, nil)
				}
			}
		}
	}
	return NewBow(series...)
}

func newSortedRandomSeries(name string, typ Type, size int, ascSort bool) Series {
	newSeries := Series{
		Name: name,
		Type: typ,
		Data: NewBuffer(size, typ, true),
	}
	switch typ {
	case Int64:
		var base int64
		for row := 0; row < size; row++ {
			if ascSort {
				newValue, _ := ToInt64(randomIncreasingNumber(typ, base))
				newSeries.Data.SetOrDrop(row, newValue)
				base += 100
			} else {
				newValue, _ := ToInt64(randomDecreasingNumber(typ, base))
				newSeries.Data.SetOrDrop(row, newValue)
				base -= 100
			}
		}
	case Float64:
		var base float64
		for row := 0; row < size; row++ {
			if ascSort {
				newValue, _ := ToFloat64(randomIncreasingNumber(typ, base))
				newSeries.Data.SetOrDrop(row, newValue)
				base += 100
			} else {
				newValue, _ := ToFloat64(randomDecreasingNumber(typ, base))
				newSeries.Data.SetOrDrop(row, newValue)
				base -= 100
			}
		}
	}
	return newSeries
}

func newRandomSeries(name string, typ Type, size int) Series {
	newSeries := Series{
		Name: name,
		Type: typ,
		Data: NewBuffer(size, typ, true),
	}
	for row := 0; row < size; row++ {
		newSeries.Data.SetOrDrop(row, randomNumber(typ))
	}
	return newSeries
}

func randomNumber(typ Type) interface{} {
	switch typ {
	case Int64:
		return rand.Int63() % 100
	case Float64:
		return rand.Float64() * 100
	default:
		panic("unsupported data type")
	}
}

func randomIncreasingNumber(typ Type, base interface{}) interface{} {
	switch typ {
	case Int64:
		base, _ := ToInt64(base)
		add, _ := ToInt64(randomNumber(Int64))
		return base + add
	case Float64:
		base, _ := ToFloat64(base)
		add, _ := ToFloat64(randomNumber(Float64))
		return base + add
	default:
		panic("unsupported data type")
	}
}

func randomDecreasingNumber(typ Type, base interface{}) interface{} {
	switch typ {
	case Int64:
		base, _ := ToInt64(base)
		add, _ := ToInt64(randomNumber(Int64))
		return base - add
	case Float64:
		base, _ := ToFloat64(base)
		add, _ := ToFloat64(randomNumber(Float64))
		return base - add
	default:
		panic("unsupported data type")
	}
}
