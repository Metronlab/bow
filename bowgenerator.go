package bow

import (
	crand "crypto/rand"
	"fmt"
	"math/big"

	"github.com/google/uuid"
)

const genDefaultNumRows = 3

// GenSeriesOptions are options to generate random Series:
// - NumRows: number of rows of the resulting Series
// - Name: name of the Series
// - Type: data type of the Series
// - GenStrategy: strategy of data generation
// - MissingData: sets whether the Series includes random nil values
type GenSeriesOptions struct {
	NumRows     int
	Name        string
	Type        Type
	GenStrategy GenStrategy
	MissingData bool
}

// NewGenBow generates a new random Bow with `numRows` rows and eventual GenSeriesOptions.
func NewGenBow(numRows int, options ...GenSeriesOptions) (Bow, error) {
	series := make([]Series, len(options))
	nameMap := make(map[string]struct{})
	for i, o := range options {
		o.NumRows = numRows
		o.validate()
		if _, ok := nameMap[o.Name]; ok {
			o.Name = fmt.Sprintf("%s_%d", o.Name, i)
		}
		nameMap[o.Name] = struct{}{}
		series[i] = o.genSeries()
	}

	return NewBow(series...)
}

// NewGenSeries returns a new randomly generated Series.
func NewGenSeries(o GenSeriesOptions) Series {
	o.validate()
	return o.genSeries()
}

func (o *GenSeriesOptions) validate() {
	if o.NumRows <= 0 {
		o.NumRows = genDefaultNumRows
	}
	if o.Name == "" {
		o.Name = "default"
	}
	if o.Type == Unknown {
		o.Type = Int64
	}
	if o.GenStrategy == nil {
		o.GenStrategy = GenStrategyIncremental
	}
}

func (o *GenSeriesOptions) genSeries() Series {
	buf := NewBuffer(o.NumRows, o.Type)
	for rowIndex := 0; rowIndex < o.NumRows; rowIndex++ {
		if !o.MissingData ||
			// 20% of nils values
			(newRandomNumber(Int64).(int64) > 2) {
			buf.SetOrDrop(rowIndex, o.GenStrategy(o.Type, rowIndex))
		}
	}

	return NewSeriesFromBuffer(o.Name, buf)
}

// GenStrategy defines how random values are generated.
type GenStrategy func(typ Type, seed int) interface{}

// GenStrategyRandom generates a random number of type `typ`.
func GenStrategyRandom(typ Type, seed int) interface{} {
	return newRandomNumber(typ)
}

// GenStrategyIncremental generates a number of type `typ` equal to the converted `seed` value.
func GenStrategyIncremental(typ Type, seed int) interface{} {
	return typ.Convert(seed)
}

// GenStrategyDecremental generates a number of type `typ` equal to the opposite of the converted `seed` value.
func GenStrategyDecremental(typ Type, seed int) interface{} {
	return typ.Convert(-seed)
}

// GenStrategyRandomIncremental generates a random number of type `typ` by using the `seed` value.
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

// GenStrategyRandomDecremental generates a random number of type `typ` by using the `seed` value.
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
	case Bool:
		return n.Int64() > 5
	case String:
		return uuid.New().String()[:8]
	default:
		panic("unsupported data type")
	}
}
