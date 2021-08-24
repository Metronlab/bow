package bow

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	"strconv"

	"github.com/google/uuid"
)

type optionGen struct {
	rows          int
	cols          int
	dataType      Type
	colNames      []string
	dataTypes     []Type
	missingData   bool
	genType       GenType
	refCol        int
	refColGenType GenType
}

// OptionGen is a type used for self-referential functions
type OptionGen func(*optionGen)

// OptionGenRows sets the number of rows for NewGenBow
func OptionGenRows(rows int) OptionGen {
	return func(f *optionGen) {
		if rows < 1 {
			panic("NewGenBow: OptionGenRows must be positive")
		}
		f.rows = rows
	}
}

// OptionGenCols sets the number of columns for NewGenBow
func OptionGenCols(cols int) OptionGen {
	return func(f *optionGen) {
		if cols < 1 {
			panic("NewGenBow: OptionGenCols must be positive")
		}
		f.cols = cols
	}
}

// OptionGenDataType sets a unique data type for every columns of the NewGenBow
func OptionGenDataType(dataType Type) OptionGen { return func(f *optionGen) { f.dataType = dataType } }

// OptionGenColNames sets the name of each column of the NewGenBow
func OptionGenColNames(colNames []string) OptionGen {
	return func(f *optionGen) { f.colNames = colNames }
}

// OptionGenDataTypes sets the data types of each column of the NewGenBow
func OptionGenDataTypes(dataTypes []Type) OptionGen {
	return func(f *optionGen) { f.dataTypes = dataTypes }
}

// OptionGenMissingData defines if the NewGenBow includes missing data at random rows in every columns except OptionGenRefCol
func OptionGenMissingData(hasMissingData bool) OptionGen {
	return func(f *optionGen) { f.missingData = hasMissingData }
}

// OptionGenRefCol defines the index of a reference column,
// which does not include missing data and is sorted for every type except bool
func OptionGenRefCol(refCol int, descSort bool) OptionGen {
	return func(f *optionGen) {
		f.refCol = refCol
	}
}

// OptionGenType defines the method used to generate values,
// default to GenTypeIncremental.
func OptionGenType(g GenType) OptionGen {
	return func(f *optionGen) {
		f.genType = g
	}
}

// NewGenBow generates a new random bow filled with the following options:
// OptionGenRows(rows int): number of rows (default 10)
// OptionGenCols(cols int): number of columns (default 10)
// OptionGenDataType(typ Type): data type (default Int64)
// OptionGenMissingData(missing bool): enable random missing data (default false)
// OptionGenRefCol(refCol int, descSort bool): defines the index of a reference column and its sorting order,
// which does not include missing data and is sorted (default refCol = -1 (no column) and descSort = false)
func NewGenBow(options ...OptionGen) (Bow, error) {
	// Set default options
	o := &optionGen{
		rows:          10,
		cols:          10,
		dataType:      Unknown,
		genType:       GenTypeIncremental,
		refCol:        -1,
		refColGenType: GenTypeIncremental,
	}
	for _, option := range options {
		option(o)
	}

	if len(o.dataTypes) > 0 && o.dataType != Unknown {
		return nil, fmt.Errorf("bow.NewGenBow: either OptionGenDataType or OptionGenDataTypes must be set")
	}
	if len(o.dataTypes) > 0 && len(o.dataTypes) != o.cols {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenDataTypes array length must be equal to OptionGenCols")
	}
	if len(o.dataTypes) == 0 && o.dataType == Unknown {
		o.dataType = Int64
	}
	if len(o.dataTypes) == 0 {
		for i := 0; i < o.cols; i++ {
			o.dataTypes = append(o.dataTypes, o.dataType)
		}
	}

	if len(o.colNames) > 0 && len(o.colNames) != o.cols {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenColNames array length must be equal to OptionGenCols")
	} else if len(o.colNames) == 0 {
		for i := 0; i < o.cols; i++ {
			o.colNames = append(o.colNames, strconv.Itoa(i))
		}
	}

	if o.refCol > o.cols-1 {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenRefCol is out of range")
	}
	if o.refCol > -1 && o.dataTypes[o.refCol] == Boolean {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenRefCol cannot be of type Boolean")
	}

	seriesSlice := make([]Series, o.cols)
	for i := range seriesSlice {
		if i == o.refCol {
			seriesSlice[i] = o.newGeneratedSeries(o.colNames[i], o.dataTypes[i], o.refColGenType)
		} else {
			seriesSlice[i] = o.newGeneratedSeries(o.colNames[i], o.dataTypes[i], o.genType)
		}
	}

	return NewBow(seriesSlice...)
}

func (o *optionGen) newGeneratedSeries(name string, typ Type, gt GenType) Series {
	buf := NewBuffer(o.rows, typ)
	for i := 0; i < o.rows; i++ {
		if !o.missingData || newRandomNumber(Boolean).(bool) {
			buf.SetOrDrop(i, gt(typ, i))
		}
	}
	return NewSeriesFromBuffer(name, buf)
}

type GenType func(typ Type, seed int) interface{}

func GenTypeRandom(typ Type, seed int) interface{} {
	return newRandomNumber(typ)
}

func GenTypeIncremental(typ Type, seed int) interface{} {
	return typ.Convert(seed)
}

func GenTypeDecremental(typ Type, seed int) interface{} {
	return typ.Convert(-seed)
}

func GenTypeIncrementalRandom(typ Type, seed int) interface{} {
	i := int64(seed) * 100
	switch typ {
	case Float64:
		add, _ := ToFloat64(newRandomNumber(Float64))
		return float64(i) + add
	default:
		add, _ := ToInt64(newRandomNumber(Int64))
		return typ.Convert(i + add)
	}
}

func GenTypeDecrementalRandom(typ Type, seed int) interface{} {
	i := -int64(seed) * 100
	switch typ {
	default:
		add, _ := ToInt64(newRandomNumber(Int64))
		return typ.Convert(i - add)
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
