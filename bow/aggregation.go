package bow

type ColumnAggregation interface {
	InputName() string
	InputIndex() int
	FromIndex(int) ColumnAggregation

	OutputName() string
	Rename(string) ColumnAggregation

	Type() Type
	Func() ColumnAggregationFunc

	// IsDeletion() bool
}

type columnAggregation struct {
	inputName  string
	inputIndex int
	outputName string
	typ        Type
	fun        ColumnAggregationFunc
	// isDeletion bool
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

// func (a columnAggregation) IsDeletion() bool {
// 	return a.isDeletion
// }

// // ColumnDeletion bypasses a column, but doesn't affect the writing of the same column via another column aggregation.
// func ColumnDeletion(name string) ColumnAggregation {
// 	return columnAggregation{
// 		outputName: name,
// 		isDeletion: true,
// 	}
// }
