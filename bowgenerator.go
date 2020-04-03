package bow

import (
	"fmt"
	"math/rand"
	"strconv"
)

// RandomBow is
type RandomBow struct {
	Rows        int
	Cols        int
	DataType    Type
	MissingData bool
	RefCol      int
	AscSort     bool
}

type Option func(*RandomBow)

func Rows(rows int) Option { return func(f *RandomBow) { f.Rows = rows } }

func Cols(cols int) Option { return func(f *RandomBow) { f.Cols = cols } }

func DataType(typ Type) Option { return func(f *RandomBow) { f.DataType = typ } }

func MissingData(missing bool) Option { return func(f *RandomBow) { f.MissingData = missing } }

func RefCol(refCol int) Option { return func(f *RandomBow) { f.RefCol = refCol } }

func DescSort(ascSort bool) Option { return func(f *RandomBow) { f.AscSort = ascSort } }

// NewRandomBow generates a new random bow filled with the following options:
//
// Rows(rows int): number of rows (default 10)
//
// Cols(cols int): number of columns (default 10)
//
// DataType(typ Type): type of data (default Int64)
//
// MissingData(missing bool): enable random missing data (default false)
//
// RefCol(refCol int): index of reference column, without missing data and sorted (default no column)
//
// DescSort(descSort bool): column index sorted in descending order (default false)
func NewRandomBow(options ...Option) (Bow, error) {
	// Default options
	f := &RandomBow{
		Rows:     10,
		Cols:     10,
		DataType: Int64,
		RefCol:   -1,
	}
	for _, option := range options {
		option(f)
	}
	if f.Rows < 1 || f.Cols < 1 {
		err := fmt.Errorf("random bow generation error: rows and cols must be positive")
		return nil, err
	}
	if f.DataType != Int64 && f.DataType != Float64 {
		err := fmt.Errorf("random bow generation error: data type must be Int64 or Float64")
		return nil, err
	}
	series := make([]Series, f.Cols)
	for i := range series {
		if i == f.RefCol {
			series[i] = newSortedRandomSeries(strconv.Itoa(i), f.DataType, f.Rows, f.AscSort)
		} else {
			series[i] = newRandomSeries(strconv.Itoa(i), f.DataType, f.Rows)
		}
	}
	if f.MissingData {
		for sIndex, s := range series {
			if sIndex != f.RefCol {
				nils := rand.Intn(f.Rows)
				for j := 0; j < nils; j++ {
					nils2 := rand.Intn(f.Rows)
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
	var basei int64
	var basef float64
	for row := 0; row < size; row++ {
		if ascSort {
			new, _ := ToInt64(randomIncreasingNumber(typ, basei))
			newSeries.Data.SetOrDrop(row, new)
			basei += 100
		} else {
			new, _ := ToFloat64(randomDecreasingNumber(typ, basef))
			newSeries.Data.SetOrDrop(row, new)
			basef -= 100
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
		panic("unknown data type")
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
		panic("unknown data type")
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
		panic("unknown data type")
	}
}
