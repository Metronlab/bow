package transformation

import (
	"fmt"

	"git.prod.metronlab.io/backend_libraries/go-bow/bow"
	"github.com/apache/arrow/go/arrow"
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

	valueType := w.Bow.Schema().Field(agg.valueCol).Type.ID()
	var sum float64

	for i := int64(0); i < w.Bow.NumRows(); i++ {
		var value float64
		raw := w.Bow.GetValue(agg.valueCol, int(i))
		switch valueType {
		case arrow.INT64:
			value = float64(raw.(int64))
		case arrow.FLOAT64:
			value = raw.(float64)
		}

		sum += value
	}

	mean := sum / float64(w.Bow.NumRows())
	b, err := w.Bow.NewColumns(
		[]interface{}{w.Start}, // todo: option to place aggregation in window
		[]interface{}{mean})

	return &bow.Window{
		Bow:   b,
		Start: w.Start,
		End:   w.End,
	}, nil
}

func (agg arithmeticMean) validate(w bow.Window) error {
	if agg.indexCol > len(w.Bow.Schema().Fields())-1 {
		return fmt.Errorf("no index column %d", agg.indexCol)
	}
	if agg.valueCol > len(w.Bow.Schema().Fields())-1 {
		return fmt.Errorf("no value column %d", agg.valueCol)
	}

	var typ arrow.Type
	typ = w.Bow.Schema().Field(agg.indexCol).Type.ID()
	if typ != arrow.INT64 {
		return fmt.Errorf("index column %d must be of type int64", agg.indexCol)
	}
	typ = w.Bow.Schema().Field(agg.valueCol).Type.ID()
	if typ != arrow.FLOAT64 &&
		typ != arrow.INT64 {
		return fmt.Errorf("value column %d must be of type int64 or float64", agg.valueCol)
	}

	return nil
}
