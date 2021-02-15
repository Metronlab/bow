package bow

import (
	crand "crypto/rand"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/google/uuid"
	"math/big"
	"strconv"
)

type randomBowOptions struct {
	Rows        int
	Cols        int
	DataType    Type
	ColNames    []string
	DataTypes   []Type
	MissingData bool
	RefCol      int
	DescSort    bool
}

// Option is a type used for self-referential functions
type Option func(*randomBowOptions)

// Rows sets the number of rows for NewRandomBow
func Rows(rows int) Option {
	return func(f *randomBowOptions) {
		if rows < 1 {
			panic("NewRandomBow: Rows must be positive")
		}
		f.Rows = rows
	}
}

// Cols sets the number of columns for NewRandomBow
func Cols(cols int) Option {
	return func(f *randomBowOptions) {
		if cols < 1 {
			panic("NewRandomBow: Cols must be positive")
		}
		f.Cols = cols
	}
}

// DataType sets a unique data type for every columns of the NewRandomBow
func DataType(dataType Type) Option { return func(f *randomBowOptions) { f.DataType = dataType } }

// ColNames sets the name of each column of the NewRandomBow
func ColNames(colNames []string) Option { return func(f *randomBowOptions) { f.ColNames = colNames } }

// DataTypes sets the data types of each column of the NewRandomBow
func DataTypes(dataTypes []Type) Option { return func(f *randomBowOptions) { f.DataTypes = dataTypes } }

// MissingData defines if the NewRandomBow includes missing data at random rows in every columns except RefCol
func MissingData(hasMissingData bool) Option {
	return func(f *randomBowOptions) { f.MissingData = hasMissingData }
}

// RefCol defines the index of a reference column, which does not include missing data and is sorted for every type except bool
func RefCol(refCol int, descSort bool) Option {
	return func(f *randomBowOptions) {
		f.RefCol = refCol
		f.DescSort = descSort
	}
}

// NewRandomBow generates a new random bow filled with the following options:
// Rows(rows int): number of rows (default 10)
// Cols(cols int): number of columns (default 10)
// DataType(typ Type): data type (default Int64)
// MissingData(missing bool): enable random missing data (default false)
// RefCol(refCol int, descSort bool): defines the index of a reference column and its sorting order,
// which does not include missing data and is sorted (default refCol = -1 (no column) and descSort = false)
func NewRandomBow(options ...Option) (Bow, error) {
	// Set default options
	f := &randomBowOptions{
		Rows:     10,
		Cols:     10,
		DataType: Unknown,
		RefCol:   -1,
	}
	for _, option := range options {
		option(f)
	}

	if len(f.DataTypes) > 0 {
		if f.DataType != Unknown {
			return nil, fmt.Errorf("NewRandomBow: either DataType or DataTypes must be set")
		} else if len(f.DataTypes) != f.Cols {
			return nil, fmt.Errorf("NewRandomBow: DataTypes array length must be equal to Cols")
		}
	} else {
		if f.DataType == Unknown {
			f.DataType = Int64
		}
		for i := 0; i < f.Cols; i++ {
			f.DataTypes = append(f.DataTypes, f.DataType)
		}
	}

	if len(f.ColNames) > 0 && len(f.ColNames) != f.Cols {
		return nil, fmt.Errorf("NewRandomBow: ColNames array length must be equal to Cols")
	} else if len(f.ColNames) == 0 {
		for i := 0; i < f.Cols; i++ {
			f.ColNames = append(f.ColNames, strconv.Itoa(i))
		}
	}

	if f.RefCol > f.Cols-1 {
		return nil, fmt.Errorf("NewRandomBow: RefCol is out of range")
	}
	if f.RefCol > -1 && f.DataTypes[f.RefCol] == Bool {
		return nil, fmt.Errorf("NewRandomBow: RefCol cannot be of type Bool")
	}

	series := make([]Series, f.Cols)
	for i := range series {
		if i == f.RefCol {
			series[i] = newSortedRandomSeries(f.ColNames[i], f.DataTypes[i], f.Rows, f.DescSort)
		} else {
			series[i] = newRandomSeries(f.ColNames[i], f.DataTypes[i], f.Rows, f.MissingData)
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
				newValue, _ := ToInt64(newRandomIncreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToInt64(newRandomDecreasingNumber(typ, base))
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
				newValue, _ := ToFloat64(newRandomIncreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToFloat64(newRandomDecreasingNumber(typ, base))
				newBuf.SetOrDrop(row, newValue)
				base += 100
			}
		}
		b.AppendValues(newBuf.Value.([]float64), newBuf.Valid)
		newArray = b.NewArray()
	case String:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		var base int64
		for row := 0; row < size; row++ {
			if descSort {
				newValue, _ := ToString(newRandomIncreasingNumber(Int64, base))
				newBuf.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToString(newRandomDecreasingNumber(Int64, base))
				newBuf.SetOrDrop(row, newValue)
				base += 100
			}
		}
		b.AppendValues(newBuf.Value.([]string), newBuf.Valid)
		newArray = b.NewArray()
	default:
		panic("unsupported data type")
	}
	return Series{
		Name:  name,
		Array: newArray,
	}
}

func newRandomSeries(name string, typ Type, size int, missingData bool) Series {
	newBuf := NewBuffer(size, typ, true)
	for row := 0; row < size; row++ {
		newBuf.SetOrDrop(row, newRandomNumber(typ))
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
	case Bool:
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		b.AppendValues(newBuf.Value.([]bool), newBuf.Valid)
		newArray = b.NewArray()
	case String:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		b.AppendValues(newBuf.Value.([]string), newBuf.Valid)
		newArray = b.NewArray()
	default:
		panic("unsupported data type")
	}
	return Series{
		Name:  name,
		Array: newArray,
	}
}

func newRandomIncreasingNumber(typ Type, base interface{}) interface{} {
	switch typ {
	case Int64:
		base, _ := ToInt64(base)
		add, _ := ToInt64(newRandomNumber(Int64))
		return base + add
	case Float64:
		base, _ := ToFloat64(base)
		add, _ := ToFloat64(newRandomNumber(Float64))
		return base + add
	default:
		panic("unsupported data type")
	}
}

func newRandomDecreasingNumber(typ Type, base interface{}) interface{} {
	switch typ {
	case Int64:
		base, _ := ToInt64(base)
		add, _ := ToInt64(newRandomNumber(Int64))
		return base - add
	case Float64:
		base, _ := ToFloat64(base)
		add, _ := ToFloat64(newRandomNumber(Float64))
		return base - add
	default:
		panic("unsupported data type")
	}
}

func newRandomNumber(typ Type) interface{} {
	n, err := crand.Int(crand.Reader, big.NewInt(100))
	if err != nil {
		panic(err)
	}
	switch typ {
	case Int64:
		return n.Int64()
	case Float64:
		return float64(n.Int64()) + 0.5
	case Bool:
		return n.Int64() > 50
	case String:
		return uuid.New().String()
	default:
		panic("unsupported data type")
	}
}
