package bow

type ColumnAggregation interface {
	InputName() string
	InputIndex() int
	FromIndex(int) ColumnAggregation

	OutputName() string
	Rename(string) ColumnAggregation

	Type() Type
	Func() ColumnAggregationFunc
}

type columnAggregation struct {
	inputName  string
	inputIndex int
	outputName string
	typ        Type
	fun        ColumnAggregationFunc
}

func NewColumnAggregation(col string, typ Type, f ColumnAggregationFunc) ColumnAggregation {
	return columnAggregation{
		inputName:  col,
		typ:        typ,
		fun:        f,
		inputIndex: -1,
	}
}

type ColumnAggregationFunc func(col int, w Window) (interface{}, error)

func (a columnAggregation) InputIndex() int {
	return a.inputIndex
}

func (a columnAggregation) InputName() string {
	return a.inputName
}

func (a columnAggregation) FromIndex(i int) ColumnAggregation {
	a.inputIndex = i
	return a
}

func (a columnAggregation) Type() Type {
	return a.typ
}

func (a columnAggregation) Func() ColumnAggregationFunc {
	return a.fun
}

func (a columnAggregation) OutputName() string {
	return a.outputName
}

func (a columnAggregation) Rename(name string) ColumnAggregation {
	a.outputName = name
	return a
}
