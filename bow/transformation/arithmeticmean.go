package transformation

import (
	"fmt"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
)

type ArithmeticMean interface {
	Transformation
}

type arithmeticMean struct {
	indexCol int
	valueCol int
}

func NewArithmeticMean(indexCol int, valueCol int) ArithmeticMean {
	return arithmeticMean{
		indexCol: indexCol,
		valueCol: valueCol,
	}
}

func (agg arithmeticMean) Apply(w bow.Window) (*bow.Window, error) {
	err := agg.validate(w)
	if err != nil {
		return nil, err
	}

	if w.Bow.NumRows() == 0 {
		return &bow.Window{
			Start: w.Start,
			End:   w.End,
			Bow:   w.Bow.NewEmpty(),
		}, nil
	}

	valueType, err := w.Bow.GetType(agg.valueCol)
	if err != nil {
		return nil, err
	}
	var sum float64

	for i := int64(0); i < w.Bow.NumRows(); i++ {
		var value float64
		raw := w.Bow.GetValue(agg.valueCol, int(i))
		switch valueType {
		case bow.Int64:
			value = float64(raw.(int64))
		case bow.Float64:
			value = raw.(float64)
		}

		sum += value
	}

	mean := sum / float64(w.Bow.NumRows()) // todo: precision
	b, err := w.Bow.NewColumns([][]interface{}{
		{w.Start}, // todo: option to place aggregation in window
		{mean}})

	return &bow.Window{
		Bow:   b,
		Start: w.Start,
		End:   w.End,
	}, nil
}

func (agg arithmeticMean) validate(w bow.Window) error {
	if agg.indexCol > w.Bow.NumSchemaCols()-1 {
		return fmt.Errorf("no index column %d", agg.indexCol)
	}
	if agg.valueCol > w.Bow.NumSchemaCols()-1 {
		return fmt.Errorf("no value column %d", agg.valueCol)
	}

	var typ bow.Type
	typ, _ = w.Bow.GetType(agg.indexCol)
	if typ != bow.Int64 {
		return fmt.Errorf("index column %d must be of type int64", agg.indexCol)
	}
	typ, _ = w.Bow.GetType(agg.valueCol)
	if typ != bow.Int64 &&
		typ != bow.Float64 {
		return fmt.Errorf("value column %d must be of type int64 or float64", agg.valueCol)
	}

	return nil
}
