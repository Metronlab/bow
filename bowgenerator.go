package bow

import (
	crand "crypto/rand"
	"fmt"
	"math/big"

	"github.com/google/uuid"
)

const (
	genDefaultNumRows = 3
)

type GenSeriesOptions struct {
	NumRows     int
	Name        string
	Type        Type
	GenStrategy GenStrategy
	MissingData bool
}

// NewGenBow generates a new random bow
func NewGenBow(numRows int, options ...GenSeriesOptions) (Bow, error) {
	seriesSlice := make([]Series, len(options))
	nameMap := make(map[string]struct{})
	for i, o := range options {
		o.NumRows = numRows
		o.validate()
		if _, ok := nameMap[o.Name]; ok {
			o.Name = fmt.Sprintf("%s_%d", o.Name, i)
		}
		nameMap[o.Name] = struct{}{}
		seriesSlice[i] = o.genSeries()
	}

	return NewBow(seriesSlice...)
}

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
	case Bool:
		return n.Int64() > 5
	case String:
		return uuid.New().String()[:8]
	default:
		panic("unsupported data type")
	}
}
