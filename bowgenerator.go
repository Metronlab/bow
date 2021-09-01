package bow

import (
	crand "crypto/rand"
	"fmt"
	"math/big"

	"github.com/google/uuid"
)

type OptionGenSeries struct {
	Rows        int
	ColName     string
	ColType     Type
	GenStrategy GenStrategy
	MissingData bool
}

const (
	genDefaultRows = 3
)

func (o *OptionGenSeries) validate() {
	if o.Rows <= 0 {
		o.Rows = genDefaultRows
	}
	if o.ColName == "" {
		o.ColName = "default"
	}
	if o.ColType == Unknown {
		o.ColType = Int64
	}
	if o.GenStrategy == nil {
		o.GenStrategy = GenStrategyIncremental
	}
}

func (o *OptionGenSeries) genSeries() Series {
	buf := NewBuffer(o.Rows, o.ColType)
	for rowIndex := 0; rowIndex < o.Rows; rowIndex++ {
		if !o.MissingData || (newRandomNumber(Int64).(int64) > 2) {
			buf.SetOrDrop(rowIndex, o.GenStrategy(o.ColType, rowIndex))
		}
	}

	return NewSeriesFromBuffer(o.ColName, buf)
}

func NewGenSeries(o OptionGenSeries) Series {
	o.validate()
	return o.genSeries()
}

// NewGenBow generates a new random bow filled with the following for each column
func NewGenBow(rowLen int, options ...OptionGenSeries) (Bow, error) {
	seriesSlice := make([]Series, len(options))
	nameMap := make(map[string]struct{})
	for i, o := range options {
		o.Rows = rowLen
		o.validate()
		if _, ok := nameMap[o.ColName]; ok {
			o.ColName = fmt.Sprintf("%s_%d", o.ColName, i)
		}
		nameMap[o.ColName] = struct{}{}
		seriesSlice[i] = o.genSeries()
	}

	return NewBow(seriesSlice...)
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
