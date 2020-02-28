package bow

import (
	"fmt"
	"math/rand"
	"strconv"
)

func NewRandomBow(rows, cols int, typ Type, missingData bool) (Bow, error) {
	if rows < 1 || cols < 1 {
		err := fmt.Errorf("random bow generation error: rows and cols must be positive")
		return nil, err
	}
	if typ != Int64 && typ != Float64 {
		err := fmt.Errorf("random bow generation error: data type must be Int64 or Float64")
		return nil, err
	}
	series := make([]Series, cols)
	for i := range series {
		series[i] = newRandomSeries(strconv.Itoa(i), typ, rows, randomNumber)
	}
	if missingData {
		for sIndex, s := range series {
			if sIndex > 0 {
				nils := rand.Intn(rows)
				for j := 0; j < nils; j++ {
					nils2 := rand.Intn(rows)
					s.Data.SetOrDrop(nils2, nil)
				}
			}
		}
	}
	return NewBow(series...)
}

func newRandomSeries(name string, typ Type, size int, f valueGenerator) Series {
	newSeries := Series{
		Name: name,
		Type: typ,
		Data: NewBuffer(size, typ, true),
	}
	for row := 0; row < size; row++ {
		newSeries.Data.SetOrDrop(row, f(typ))
	}
	return newSeries
}

type valueGenerator func(typ Type) interface{}

func randomNumber(typ Type) interface{} {
	switch typ {
	case Int64:
		return rand.Int63() * rand.Int63() % 100
	case Float64:
		return rand.Float64() * 100
	default:
		panic("unknown data type")
	}
}
