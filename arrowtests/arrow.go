package arrowtests

import (
	"fmt"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
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
func NewTSRecord() (*arrow.Schema, arrow.Record) {
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, EventSchema)
	defer b.Release()

	b.Field(0).(*array.Time32Builder).AppendValues([]arrow.Time32{1, 2, 3, 4}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{42, 42, 41, 42}, nil)

	return EventSchema, b.NewRecord()
}

//PrintRecordColumns Print a columns based output
func PrintRecordColumns(rec arrow.Record) {
	for i, col := range rec.Columns() {
		fmt.Printf("column[%d] %q: %v\n", i, rec.ColumnName(i), col)
	}
}

//PrintRecordRows Print a row based output
func PrintRecordRows(schema *arrow.Schema, recs []arrow.Record) {
	// Make a table read only based on many records
	table := array.NewTableFromRecords(schema, recs)
	defer table.Release()

	// makes a events series
	events := make([]Event, table.NumRows())

	// Seek schema index for event
	timeIndex := table.Schema().FieldIndices("time")[0]
	valueIndex := table.Schema().FieldIndices("value")[0]
	qualityIndex := table.Schema().FieldIndices("quality")[0]

	// TableReader is able to iter on a table grouping by indexes,
	// marvelous to do calculation in parallel
	// Underutilized in this case, for a naive implementation iteration is done 1 by 1
	tr := array.NewTableReader(table, 1)
	defer tr.Release()

	// fill series with TableReader iteration
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

	// Prints series
	for _, e := range events {
		fmt.Println("time:", e.Time, ", value:", e.Value, ", quality:", e.quality)
	}
}
