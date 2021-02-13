package bow

import (
	crand "crypto/rand"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"math/big"
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
			series[i] = newRandomSeries(strconv.Itoa(i), f.DataType, f.Rows, f.MissingData)
		}
	}
	return NewBow(series...)
}

func newSortedRandomSeries(name string, typ Type, size int, descSort bool) Series {
	var newArray array.Interface
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	newBuf := NewBuffer(size, typ, true)
	switch typ {
	case Int64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		var base int64
		for row := 0; row < size; row++ {
			if descSort {
				newValue, _ := ToInt64(randomIncreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToInt64(randomDecreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base += 100
			}
		}
		b.AppendValues(newBuf.Value.([]int64), newBuf.Valid)
		newArray = b.NewArray()
	case Float64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		var base float64
		for row := 0; row < size; row++ {
			if descSort {
				newValue, _ := ToFloat64(randomIncreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToFloat64(randomDecreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base += 100
			}
		}
		b.AppendValues(newBuf.Value.([]float64), newBuf.Valid)
		newArray = b.NewArray()
	}
	return Series{
		Name:  name,
		Array: newArray,
	}
}

func newRandomSeries(name string, typ Type, size int, missingData bool) Series {
	newBuf := NewBuffer(size, typ, true)
	for row := 0; row < size; row++ {
		newBuf.SetOrDrop(row, randomNumber(typ))
	}
	if missingData {
		nils, err := crand.Int(crand.Reader, big.NewInt(int64(size)))
		if err != nil {
			panic(err)
		}
		for j := 0; j < int(nils.Int64()); j++ {
			nils2, err := crand.Int(crand.Reader, big.NewInt(int64(size)))
			if err != nil {
				panic(err)
			}
			newBuf.SetOrDrop(int(nils2.Int64()), nil)
		}
	}
	var newArray array.Interface
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	switch typ {
	case Float64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		b.AppendValues(newBuf.Value.([]float64), newBuf.Valid)
		newArray = b.NewArray()
	case Int64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendValues(newBuf.Value.([]int64), newBuf.Valid)
		newArray = b.NewArray()
	}
	return Series{
		Name:  name,
		Array: newArray,
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

func randomNumber(typ Type) interface{} {
	n, err := crand.Int(crand.Reader, big.NewInt(100))
	if err != nil {
		panic(err)
	}
	switch typ {
	case Int64:
		return n.Int64()
	case Float64:
		return float64(n.Int64()) + 0.5
	default:
		panic("unsupported data type")
	}
}
