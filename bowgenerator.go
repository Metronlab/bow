package bow

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	"strconv"

	"github.com/google/uuid"
)

type optionGen struct {
	rows            int
	cols            int
	colNames        []string
	colTypes        []Type
	genStrategies   []GenStrategy
	missingDataCols []int
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

// OptionGenColNames sets the names of each column of a NewGenBow
func OptionGenColNames(colNames []string) OptionGen {
	return func(f *optionGen) { f.colNames = colNames }
}

// OptionGenColTypes sets the data types of each column of a NewGenBow
func OptionGenColTypes(colTypes []Type) OptionGen {
	return func(f *optionGen) { f.colTypes = colTypes }
}

// OptionGenStrategies defines the strategy used to generate values for each column,
// default to GenStrategyIncremental.
func OptionGenStrategies(s []GenStrategy) OptionGen {
	return func(f *optionGen) { f.genStrategies = s }
}

// OptionGenMissingData defines if the NewGenBow includes missing data at random rows in selected columns
func OptionGenMissingData(colIndices []int) OptionGen {
	return func(f *optionGen) { f.missingDataCols = colIndices }
}

const (
	genDefaultRows = 3
	genDefaultCols = 2
)

// NewGenBow generates a new random bow filled with the following options:
// OptionGenRows(rows int): sets the number of rows (default 3)
// OptionGenCols(cols int): sets the number of columns (default 3)
// OptionGenColNames(colNames []string): sets the names of each column (default colIndex)
// OptionGenColTypes(colTypes []Type): sets data types of columns (default Int64)
// OptionGenStrategies(s []GenStrategy): sets the data generation strategies (default GenStrategyIncremental)
// OptionGenMissingData(colIndices []int): enables random missing data (default false)
func NewGenBow(options ...OptionGen) (Bow, error) {
	// Set default options
	o := &optionGen{
		rows: genDefaultRows,
		cols: genDefaultCols,
	}
	for _, option := range options {
		option(o)
	}

	if len(o.colNames) > 0 && len(o.colNames) != o.cols {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenColNames array length must be equal to OptionGenCols")
	} else if len(o.colNames) == 0 {
		for i := 0; i < o.cols; i++ {
			o.colNames = append(o.colNames, strconv.Itoa(i))
		}
	}

	if len(o.colTypes) > 0 && len(o.colTypes) != o.cols {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenColTypes array length must be equal to OptionGenCols")
	}
	if len(o.colTypes) == 0 {
		for i := 0; i < o.cols; i++ {
			o.colTypes = append(o.colTypes, Int64)
		}
	}

	if len(o.genStrategies) > 0 && len(o.genStrategies) != o.cols {
		return nil, fmt.Errorf("bow.NewGenBow: OptionGenStrategies array length must be equal to OptionGenCols")
	}
	if len(o.genStrategies) == 0 {
		for i := 0; i < o.cols; i++ {
			o.genStrategies = append(o.genStrategies, GenStrategyIncremental)
		}
	}

	seriesSlice := make([]Series, o.cols)
	for seriesIndex := range seriesSlice {
		missingData := false
		for _, colIndex := range o.missingDataCols {
			if seriesIndex == colIndex {
				missingData = true
			}
		}
		seriesSlice[seriesIndex] = o.newGeneratedSeries(
			o.colNames[seriesIndex],
			o.colTypes[seriesIndex],
			o.genStrategies[seriesIndex],
			missingData)
	}

	return NewBow(seriesSlice...)
}

func (o *optionGen) newGeneratedSeries(name string, typ Type, s GenStrategy, missingData bool) Series {
	buf := NewBuffer(o.rows, typ)
	for rowIndex := 0; rowIndex < o.rows; rowIndex++ {
		if !missingData || (newRandomNumber(Int64).(int64) > 2) {
			buf.SetOrDrop(rowIndex, s(typ, rowIndex))
		}
	}

	return NewSeriesFromBuffer(name, buf)
}

type GenStrategy func(typ Type, seed int) interface{}

func GenStrategyRandom(typ Type, seed int) interface{} {
	return newRandomNumber(typ)
}

func GenStrategyIncremental(typ Type, seed int) interface{} {
	return typ.Convert(seed)
}

func GenStrategyDecremental(typ Type, seed int) interface{} {
	return typ.Convert(-seed)
}

func GenStrategyRandomIncremental(typ Type, seed int) interface{} {
	i := int64(seed) * 10
	switch typ {
	case Float64:
		add, _ := ToFloat64(newRandomNumber(Float64))
		return float64(i) + add
	default:
		add, _ := ToInt64(newRandomNumber(Int64))
		return typ.Convert(i + add)
	}
}

func GenStrategyRandomDecremental(typ Type, seed int) interface{} {
	i := -int64(seed) * 10
	switch typ {
	default:
		add, _ := ToInt64(newRandomNumber(Int64))
		return typ.Convert(i - add)
	}
}

func newRandomNumber(typ Type) interface{} {
	n, err := crand.Int(crand.Reader, big.NewInt(10))
	if err != nil {
		panic(err)
	}
	switch typ {
	case Int64:
		return n.Int64()
	case Float64:
		return float64(n.Int64()) + 0.5
	case Boolean:
		return n.Int64() > 5
	case String:
		return uuid.New().String()[:8]
	default:
		panic("unsupported data type")
	}
}
