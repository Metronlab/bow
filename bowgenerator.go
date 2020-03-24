package bow

import (
	"fmt"
	"math/rand"
	"strconv"
)

// NewRandomBow generates a new bow filled with random values of type typ, with or without missing data
// and can include a reference column without missing data which is sorted
func NewRandomBow(rows, cols int, typ Type, missingData bool, refCol int, ascSort bool) (Bow, error) {
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
		if i == refCol {
			series[i] = newSortedRandomSeries(strconv.Itoa(i), typ, rows, ascSort)
		} else {
			series[i] = newRandomSeries(strconv.Itoa(i), typ, rows)
		}
	}
	if missingData {
		for sIndex, s := range series {
			if sIndex != refCol {
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
