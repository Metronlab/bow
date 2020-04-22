package bow

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

// Bow is a wrapper of apache arrow array record.
// It was not implemented as a facade shadowing arrow
// in order to expose low lvl arrow decisions to bow users
// while arrow is in beta
type Bow interface {
	// Meeting Stringer interface
	String() string

	// Getters
	GetType(colIndex int) Type
	GetName(colIndex int) (string, error)
	GetIndex(colName string) (int, error)
	GetColNameIndex(string) int

	GetRow(rowIndex int) map[string]interface{}

	GetValueByName(colName string, rowIndex int) interface{}
	GetValue(colIndex, rowIndex int) interface{}
	GetNextValue(col, row int) (v interface{}, resultsRow int)
	GetNextValues(col1, col2, row int) (v1, v2 interface{}, resultsRow int)
	GetPreviousValue(col, row int) (v interface{}, resultsRow int)
	GetPreviousValues(col1, col2, row int) (v1, v2 interface{}, resultsRow int)

	GetInt64(colIndex, rowIndex int) (int64, bool)
	GetNextInt64(col, row int) (v int64, resultsRow int)
	GetPreviousInt64(col, row int) (v int64, resultsRow int)

	GetFloat64(colIndex, rowIndex int) (float64, bool)
	GetNextFloat64(col, row int) (v float64, resultsRow int)
	GetNextFloat64s(col1, col2, row int) (v1, v2 float64, resultsRow int)
	GetPreviousFloat64(col, row int) (v float64, resultsRow int)
	GetPreviousFloat64s(col1, col2, row int) (v1, v2 float64, resultsRow int)

	// Iterators
	RowMapIter() chan map[string]interface{}

	// Joins
	InnerJoin(other Bow) Bow
	OuterJoin(other Bow) Bow

	Equal(Bow) bool
	// todo: design and rethink:
	// Merge(bows ...Bow) (Bow, error)

	SetMarshalJSONRowBased(rowOriented bool)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error

	NewSlice(i, j int) Bow
	NewValues(columns [][]interface{}) (bobow Bow, err error)
	NewEmpty() Bow
	DropNil(nilCols ...string) (Bow, error)

	// Handling missing data
	FillPrevious(colNames ...string) (Bow, error)
	FillNext(colNames ...string) (Bow, error)
	FillMean(colNames ...string) (Bow, error)
	FillLinear(refCol string, toFillCol string) (Bow, error)

	// Exposed from arrow.Record
	Release()
	Retain()
	Schema() *arrow.Schema
	Column(i int) array.Interface
	NumRows() int
	NumCols() int

	IsColEmpty(colIndex int) bool
	IsColSorted(colIndex int) bool
}

type bow struct {
	marshalJSONRowBased bool
	array.Record
}

func NewBow(series ...Series) (bobow Bow, err error) {
	if len(series) == 0 {
		bobow = &bow{}
		return
	}
	var fields []arrow.Field
	var cols []array.Interface
	var nrows int64
	if series != nil && series[0].Array != nil {
		nrows = int64(series[0].Array.Len())
	}
	for _, s := range series {
		if s.Array == nil {
			err = errors.New("bow: NewBow: empty Series")
			return
		}
		if s.Name == "" {
			err = errors.New("bow: empty Series name")
			return
		}
		if getTypeFromArrowType(s.Array.DataType()) == Unknown {
			err = fmt.Errorf("bow: unsupported type: %s", s.Array.DataType().Name())
			return
		}
		if int64(s.Array.Len()) != nrows {
			err = fmt.Errorf("bow: Series '%s' has a length of %d, which is different from the previous ones",
				s.Name, s.Array.Len())
			return
		}
		newField := arrow.Field{
			Name: s.Name,
			Type: s.Array.DataType(),
		}
		fields = append(fields, newField)
		cols = append(cols, s.Array)
	}
	schema := arrow.NewSchema(fields, nil)
	bobow = &bow{
		Record: array.NewRecord(schema, cols, nrows),
	}
	return
}

func NewBowFromColumnBasedInterfaces(columnsNames []string, types []Type, columns [][]interface{}) (bobow Bow, err error) {
	if len(columnsNames) != len(columns) {
		err = errors.New("bow: columnsNames and columns array lengths don't match")
		return
	}

	if types != nil && len(columnsNames) != len(types) {
		err = errors.New("bow: columnsNames and types array lengths don't match")
		return
	}

	series := make([]Series, len(columnsNames))
	for i, name := range columnsNames {
		if types != nil {
			series[i], err = NewSeriesFromInterfaces(name, types[i], columns[i])
		} else {
			series[i], err = NewSeriesFromInterfaces(name, Unknown, columns[i])
		}
		if err != nil {
			return
		}
	}
	return NewBow(series...)
}

func NewBowFromRowBasedInterfaces(columnsNames []string, types []Type, rows [][]interface{}) (bobow Bow, err error) {
	if len(rows) <= 0 {
		err = errors.New("bow: empty rows")
		return
	}
	columnBasedRows := make([][]interface{}, len(columnsNames))
	for column := range columnsNames {
		columnBasedRows[column] = make([]interface{}, len(rows))
	}
	for rowI, row := range rows {
		if len(columnsNames) < len(row) {
			err = errors.New("bow: mismatch between columnsNames names and row len")
			return
		}
		for colI := range columnsNames {
			columnBasedRows[colI][rowI] = row[colI]
		}
	}
	return NewBowFromColumnBasedInterfaces(columnsNames, types, columnBasedRows)
}

func AppendBows(bows ...Bow) (bobow Bow, err error) {
	if len(bows) == 0 {
		return
	}
	if len(bows) == 1 {
		bobow = bows[0]
		return
	}
	refBow := bows[0]
	refSchema := refBow.Schema()
	var numRows int
	for _, b := range bows {
		schema := b.Schema()
		if !schema.Equal(refSchema) {
			err = fmt.Errorf("schema mismatch: got both\n%v\nand\n%v", refSchema, schema)
			return
		}
		numRows += b.NumRows()
	}

	seriess := make([]Series, refBow.NumCols())
	bufs := make([]Buffer, refBow.NumCols())
	var name string
	for ci := 0; ci < refBow.NumCols(); ci++ {
		var rowOffset int
		typ := refBow.GetType(ci)
		name, err = refBow.GetName(ci)
		if err != nil {
			return
		}
		bufs[ci] = NewBuffer(numRows, typ, true)
		for _, b := range bows {
			for ri := 0; ri < b.NumRows(); ri++ {
				bufs[ci].SetOrDrop(ri+rowOffset, b.GetValue(ci, ri))
			}
			rowOffset += b.NumRows()
		}

		seriess[ci] = NewSeries(name, typ, bufs[ci].Value, bufs[ci].Valid)
	}
	return NewBow(seriess...)
}

func (b *bow) NewEmpty() Bow {
	return b.NewSlice(0, 0)
}

func (b *bow) NewValues(columns [][]interface{}) (Bow, error) {
	if len(columns) != b.NumCols() {
		return nil, errors.New("bow: mismatch between schema and data")
	}
	seriess := make([]Series, len(columns))
	for i, c := range columns {
		typ := b.GetType(i)
		buf, err := NewBufferFromInterfaces(typ, c)
		if err != nil {
			return nil, err
		}
		seriess[i] = NewSeries(b.Schema().Field(i).Name, typ, buf.Value, buf.Valid)
	}
	return NewBow(seriess...)
}

// DropNil drops any row that contains a nil for any of `nilCols`.
// `nilCols` defaults to all columns.
func (b *bow) DropNil(nilCols ...string) (Bow, error) {
	// default = all columns
	if len(nilCols) == 0 {
		for _, field := range b.Schema().Fields() {
			nilCols = append(nilCols, field.Name)
		}
	} else {
		nilCols = dedupStrings(nilCols)
	}

	nilColIndexes := make([]int, len(nilCols))
	for i := 0; i < len(nilCols); i++ {
		var err error
		nilColIndexes[i], err = b.GetIndex(nilCols[i])
		if err != nil {
			return nil, err
		}
	}

	var dropped []int
	for ri := 0; ri < b.NumRows(); ri++ {
		for _, ci := range nilColIndexes {
			if b.GetValue(ci, ri) == nil {
				dropped = append(dropped, ri)
				break
			}
		}
	}

	if len(dropped) == 0 {
		return b, nil
	}

	slices := make([]Bow, len(dropped)+1)
	var curr int
	for i, di := range dropped {
		slices[i] = b.NewSlice(curr, di)
		curr = di + 1
	}
	slices[len(dropped)] = b.NewSlice(curr, b.NumRows())

	return AppendBows(slices...)
}

func dedupStrings(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	writeIndex := 0
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		s[writeIndex] = v
		writeIndex++
	}
	return s[:writeIndex]
}

func (b *bow) String() string {
	if b.Record == nil {
		return ""
	}
	w := new(tabwriter.Writer)
	writer := new(strings.Builder)
	// tabs will be replaced by two spaces by formatter
	w.Init(writer, 0, 4, 2, ' ', 0)

	// format any line (header or row)
	formatRow := func(getCellStr func(colIndex int) string) {
		var cells []string
		for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
			cells = append(cells, fmt.Sprintf("%v", getCellStr(colIndex)))
		}
		_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
		if err != nil {
			panic(err)
		}
	}

	// Print col names on buffer
	formatRow(func(colIndex int) string {
		return fmt.Sprintf("%s:%v", b.Schema().Field(colIndex).Name, b.GetType(colIndex))
	})

	// Print each row on buffer
	rowChan := b.RowMapIter()
	for row := range rowChan {
		formatRow(func(colIndex int) string {
			return fmt.Sprintf("%v", row[b.Schema().Field(colIndex).Name])
		})
	}

	// Flush buffer and format lines along the way
	if err := w.Flush(); err != nil {
		panic(err)
	}

	return writer.String()
}

func (b *bow) RowMapIter() chan map[string]interface{} {
	mapChan := make(chan map[string]interface{})
	go b.rowMapIter(mapChan)
	return mapChan
}

func (b *bow) rowMapIter(mapChan chan map[string]interface{}) {
	defer close(mapChan)

	if b.Record == nil || b.NumRows() <= 0 {
		return
	}

	for rowIndex := 0; rowIndex < b.NumRows(); rowIndex++ {
		mapChan <- b.GetRow(rowIndex)
	}
}

func (b *bow) Equal(B2 Bow) bool {
	b2, ok := B2.(*bow)
	if !ok {
		panic("bow: cannot Equal on non bow object")
	}

	if b.Record == nil && b2.Record == nil {
		return true
	}
	if b.Record == nil && b2.Record != nil {
		return false
	}
	if b2.Record == nil && b.Record != nil {
		return false
	}

	if !b.Schema().Equal(b2.Schema()) {
		return false
	}

	b1Chan := b.RowMapIter()
	b2Chan := b2.RowMapIter()
	for {
		i1, ok1 := <-b1Chan
		i2, ok2 := <-b2Chan
		for len(i1) == 0 && ok1 {
			i1, ok1 = <-b1Chan
		}
		for len(i2) == 0 && ok2 {
			i2, ok2 = <-b2Chan
		}
		if ok1 != ok2 {
			return false
		}
		if !ok1 && !ok2 {
			break
		}
		if !reflect.DeepEqual(i1, i2) {
			return false
		}
	}
	return true
}

func (b *bow) NewSlice(i, j int) Bow {
	return &bow{
		Record: b.Record.NewSlice(int64(i), int64(j)),
	}
}

func (b *bow) NumRows() int {
	if b.Record == nil {
		return 0
	}
	return int(b.Record.NumRows())
}

func (b *bow) NumCols() int {
	if b.Record == nil {
		return 0
	}
	return int(b.Record.NumCols())
}
