package bow

type ColumnAggregationFunc func(col int, w Window) (interface{}, error)

type ColumnAggregation struct {
	Type Type
	Func ColumnAggregationFunc
	name string
}

func (a ColumnAggregation) GetName() string {
	return a.name
}

func (a ColumnAggregation) SetName(name string) ColumnAggregation {
	a.name = name
	return a
}
