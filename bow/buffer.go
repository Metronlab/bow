package bow

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	ErrBufferOverload = "bow: buffer cannot contains all data being sent in channel"
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
	valid := make([]bool, length)
	i := 0
	switch t {
	case Unknown:
		return Buffer{}, errors.New("bow: cannot create buffer of unknown type")
	case Float64:
		vv := make([]float64, length)
		for c := range cells {
			if i >= length {
				return Buffer{}, errors.New(ErrBufferOverload)
			}
			switch c := c.(type) {
			case float64:
				vv[i], valid[i] = c, true
			case json.Number:
				f, err := c.Float64()
				if err != nil {
					break
				}
				vv[i], valid[i] = f, true
			}
			i++
		}
		return Buffer{Value: vv, Valid: valid}, nil
	case Int64:
		vv := make([]int64, length)
		for c := range cells {
			if i >= length {
				return Buffer{}, errors.New(ErrBufferOverload)
			}
			switch c := c.(type) {
			case int:
				vv[i], valid[i] = int64(c), true
			case json.Number:
				f, err := c.Int64()
				if err != nil {
					break
				}
				vv[i], valid[i] = f, true
			case int64:
				vv[i], valid[i] = c, true
			}
			i++
		}
		return Buffer{Value: vv, Valid: valid}, nil
	case Bool:
		vv := make([]bool, length)
		for c := range cells {
			if i >= length {
				return Buffer{}, errors.New(ErrBufferOverload)
			}
			vv[i], valid[i] = c.(bool)
			i++
		}
		return Buffer{Value: vv, Valid: valid}, nil
	}
	return Buffer{}, fmt.Errorf("bow: unhandled type number: %v", t)
}

func (b *Buffer) SetOrDrop(i int, value interface{}) {
	switch v := b.Value.(type) {
	case []int64:
		v[i], b.Valid[i] = Int64.Convert(value).(int64)
	case []float64:
		v[i], b.Valid[i] = Float64.Convert(value).(float64)
	case []bool:
		v[i], b.Valid[i] = Bool.Convert(value).(bool)
	default:
		panic(fmt.Errorf("unsuported buffer type %T", v))
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
				return Unknown, errors.New("strings are not handled yet")
			case bool:
				return Bool, nil
			}
		}
	}
	return Float64, nil
}
