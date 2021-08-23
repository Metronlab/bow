package bow

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	"strconv"

	"github.com/google/uuid"
)

type genBowOptions struct {
	rows        int
	cols        int
	dataType    Type
	colNames    []string
	dataTypes   []Type
	missingData bool
	refCol      int
	descSort    bool
}

// Option is a type used for self-referential functions
type Option func(*genBowOptions)

// GenRows sets the number of rows for NewGenBow
func GenRows(rows int) Option {
	return func(f *genBowOptions) {
		if rows < 1 {
			panic("NewGenBow: GenRows must be positive")
		}
		f.rows = rows
	}
}

// GenCols sets the number of columns for NewGenBow
func GenCols(cols int) Option {
	return func(f *genBowOptions) {
		if cols < 1 {
			panic("NewGenBow: GenCols must be positive")
		}
		f.cols = cols
	}
}

// GenDataType sets a unique data type for every columns of the NewGenBow
func GenDataType(dataType Type) Option { return func(f *genBowOptions) { f.dataType = dataType } }

// GenColNames sets the name of each column of the NewGenBow
func GenColNames(colNames []string) Option { return func(f *genBowOptions) { f.colNames = colNames } }

// GenDataTypes sets the data types of each column of the NewGenBow
func GenDataTypes(dataTypes []Type) Option { return func(f *genBowOptions) { f.dataTypes = dataTypes } }

// GenMissingData defines if the NewGenBow includes missing data at random rows in every columns except GenRefCol
func GenMissingData(hasMissingData bool) Option {
	return func(f *genBowOptions) { f.missingData = hasMissingData }
}

// GenRefCol defines the index of a reference column,
// which does not include missing data and is sorted for every type except bool
func GenRefCol(refCol int, descSort bool) Option {
	return func(f *genBowOptions) {
		f.refCol = refCol
		f.descSort = descSort
	}
}

// NewGenBow generates a new random bow filled with the following options:
// GenRows(rows int): number of rows (default 10)
// GenCols(cols int): number of columns (default 10)
// GenDataType(typ Type): data type (default Int64)
// GenMissingData(missing bool): enable random missing data (default false)
// GenRefCol(refCol int, descSort bool): defines the index of a reference column and its sorting order,
// which does not include missing data and is sorted (default refCol = -1 (no column) and descSort = false)
func NewGenBow(options ...Option) (Bow, error) {
	// Set default options
	f := &genBowOptions{
		rows:     10,
		cols:     10,
		dataType: Unknown,
		refCol:   -1,
	}
	for _, option := range options {
		option(f)
	}

	if len(f.dataTypes) > 0 && f.dataType != Unknown {
		return nil, fmt.Errorf("bow.NewGenBow: either GenDataType or GenDataTypes must be set")
	}
	if len(f.dataTypes) > 0 && len(f.dataTypes) != f.cols {
		return nil, fmt.Errorf("bow.NewGenBow: GenDataTypes array length must be equal to GenCols")
	}
	if len(f.dataTypes) == 0 && f.dataType == Unknown {
		f.dataType = Int64
	}
	if len(f.dataTypes) == 0 {
		for i := 0; i < f.cols; i++ {
			f.dataTypes = append(f.dataTypes, f.dataType)
		}
	}

	if len(f.colNames) > 0 && len(f.colNames) != f.cols {
		return nil, fmt.Errorf("bow.NewGenBow: GenColNames array length must be equal to GenCols")
	} else if len(f.colNames) == 0 {
		for i := 0; i < f.cols; i++ {
			f.colNames = append(f.colNames, strconv.Itoa(i))
		}
	}

	if f.refCol > f.cols-1 {
		return nil, fmt.Errorf("bow.NewGenBow: GenRefCol is out of range")
	}
	if f.refCol > -1 && f.dataTypes[f.refCol] == Boolean {
		return nil, fmt.Errorf("bow.NewGenBow: GenRefCol cannot be of type Boolean")
	}

	seriesSlice := make([]Series, f.cols)
	for i := range seriesSlice {
		if i == f.refCol {
			seriesSlice[i] = newSortedRandomSeries(f.colNames[i], f.dataTypes[i], f.rows, f.descSort)
		} else {
			seriesSlice[i] = newRandomSeries(f.colNames[i], f.dataTypes[i], f.rows, f.missingData)
		}
	}

	return NewBow(seriesSlice...)
}

func newSortedRandomSeries(name string, typ Type, size int, descSort bool) Series {
	series := NewSeries(name, size, typ)
	switch typ {
	case Int64:
		var base int64
		for row := 0; row < size; row++ {
			if descSort {
				newValue, _ := ToInt64(newRandomIncreasingNumber(typ, base))
				series.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToInt64(newRandomDecreasingNumber(typ, base))
				series.SetOrDrop(row, newValue)
				base += 100
			}
		}
	case Float64:
		var base float64
		for row := 0; row < size; row++ {
			if descSort {
				newValue, _ := ToFloat64(newRandomIncreasingNumber(typ, base))
				series.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToFloat64(newRandomDecreasingNumber(typ, base))
				series.SetOrDrop(row, newValue)
				base += 100
			}
		}
	case String:
		var base int64
		for row := 0; row < size; row++ {
			if descSort {
				newValue, _ := ToString(newRandomIncreasingNumber(Int64, base))
				series.SetOrDrop(row, newValue)
				base -= 100
			} else {
				newValue, _ := ToString(newRandomDecreasingNumber(Int64, base))
				series.SetOrDrop(row, newValue)
				base += 100
			}
		}
	default:
		panic("unsupported data type")
	}

	return series
}

func newRandomSeries(name string, typ Type, size int, missingData bool) Series {
	series := NewSeries(name, size, typ)
	for row := 0; row < size; row++ {
		series.SetOrDrop(row, newRandomNumber(typ))
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
			series.SetOrDrop(int(nils2.Int64()), nil)
		}
	}

	return series
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
	case Boolean:
		return n.Int64() > 50
	case String:
		return uuid.New().String()
	default:
		panic("unsupported data type")
	}
}
