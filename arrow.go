package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pkg/errors"
)

var (
	EventSchema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "time", Type: arrow.FixedWidthTypes.Time32ms},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "quality", Type: arrow.PrimitiveTypes.Int64},
		}, nil,
	)
)

type Event struct {
	Time    arrow.Time32
	Value   interface{}
	quality int64
}

//NewTSRecord Create a new sample base on eventSchema
func NewTSRecord() (*arrow.Schema, array.Record) {
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, EventSchema)
	defer b.Release()

	b.Field(0).(*array.Time32Builder).AppendValues([]arrow.Time32{1, 2, 3, 4}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{42, 42, 41, 42}, nil)

	return EventSchema, b.NewRecord()
}

//PrintRecordColumns Print a columns based output
func PrintRecordColumns(rec array.Record) {
	for i, col := range rec.Columns() {
		fmt.Printf("column[%d] %q: %v\n", i, rec.ColumnName(i), col)
	}
}

//PrintRecordRows Print a row based output
func PrintRecordRows(schema *arrow.Schema, recs []array.Record) {
	// Make a table read only based on many records
	table := array.NewTableFromRecords(schema, recs)
	defer table.Release()

	// makes a events buffer
	events := make([]Event, table.NumRows())

	// Seek schema index for event
	timeIndex := table.Schema().FieldIndex("time")
	if timeIndex < 0 {
		panic(errors.New("impossible to convert record to event"))
	}
	valueIndex := table.Schema().FieldIndex("value")
	if valueIndex < 0 {
		panic(errors.New("impossible to convert record to event"))
	}
	qualityIndex := table.Schema().FieldIndex("quality")
	if qualityIndex < 0 {
		panic(errors.New("impossible to convert record to event"))
	}

	// TableReader is able to iter on a table grouping by indexes,
	// marvellous to do calculation in parallel
	// Underutilized in this case, for a naive implementation iteration is done 1 by 1
	tr := array.NewTableReader(table, 1)
	defer tr.Release()

	// fill buffer with TableReader iteration
	index := 0
	for tr.Next() {
		rec := tr.Record()

		td := array.NewTime32Data(rec.Column(timeIndex).Data())
		if td.IsValid(0) {
			events[index].Time = td.Time32Values()[0]
		}
		vd := array.NewFloat64Data(rec.Column(valueIndex).Data())
		if vd.IsValid(0) {
			events[index].Value = vd.Float64Values()[0]
		}
		qd := array.NewInt64Data(rec.Column(qualityIndex).Data())
		if qd.IsValid(0) {
			events[index].quality = qd.Int64Values()[0]
		}

		index++
	}

	// Prints buffer
	for _, e := range events {
		fmt.Println("time:", e.Time, ", value:", e.Value, ", quality:", e.quality)
	}
}

func merge() {
	// Seems to be able to join
	// poc to do
	//array.NewTableFromRecords()
}
