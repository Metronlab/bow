package bow

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Buffer struct {
	Value interface{}
	Valid []bool
}

func NewBuffer(size int, t Type, nullable bool) Buffer {
	var valid []bool
	if nullable {
		valid = make([]bool, size)
	}
	switch t {
	case Float64:
		return Buffer{
			Value: make([]float64, size),
			Valid: valid,
		}
	case Int64:
		return Buffer{
			Value: make([]int64, size),
			Valid: valid,
		}
	case Bool:
		return Buffer{
			Value: make([]bool, size),
			Valid: valid,
		}
	case String:
		return Buffer{
			Value: make([]string, size),
			Valid: valid,
		}
	default:
		panic(fmt.Errorf("unknown type for buffer: %v", t))
	}
}

func NewBufferFromInterfaces(t Type, cells []interface{}) (Buffer, error) {
	return NewBufferFromInterfacesIter(t, len(cells), func() chan interface{} {
		cellsChan := make(chan interface{})
		go func() {
			for _, c := range cells {
				cellsChan <- c
			}
			close(cellsChan)
		}()
		return cellsChan
	}())
}

func NewBufferFromInterfacesIter(t Type, length int, cells chan interface{}) (Buffer, error) {
	if !t.IsSupported() {
		return Buffer{}, errors.New("bow: cannot create buffer of unknown type")
	}
	buff := NewBuffer(length, t, true)
	i := 0
	for c := range cells {
		buff.SetOrDrop(i, c)
		i++
	}
	return buff, nil
}

func (b *Buffer) SetOrDrop(i int, value interface{}) {
	switch v := b.Value.(type) {
	case []int64:
		v[i], b.Valid[i] = Int64.Convert(value).(int64)
	case []float64:
		v[i], b.Valid[i] = Float64.Convert(value).(float64)
	case []bool:
		v[i], b.Valid[i] = Bool.Convert(value).(bool)
	case []string:
		v[i], b.Valid[i] = String.Convert(value).(string)
	default:
		panic(fmt.Errorf("unsupported buffer type %T", v))
	}
}

func seekType(cells []interface{}) (Type, error) {
	for _, val := range cells {
		if val != nil {
			switch val.(type) {
			case float64, json.Number:
				return Float64, nil
			case int, int64:
				return Int64, nil
			case string:
				return String, nil
			case bool:
				return Bool, nil
			}
		}
	}
	return Float64, nil
}
