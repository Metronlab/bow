package bow

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"
)

//Bow is a wrapper of apache arrow array record.
// It was not implemented as a facade shadowing arrow
// in order to expose low lvl arrow decisions to bow users
// while arrow is in beta
type Bow interface {
	PrintRows()
	RowMapIter() chan map[string]interface{}

	//UnmarshalJSON([]byte) error
	//MarshalJSON() ([]byte, error)

	// todo: design and rethink:
	// Merge(bows ...Bow) (Bow, error)

	// Surcharged on Record:
	NewSlice(i, j int64) Bow

	// Exposed from Record:
	Release()
	Retain()

	NumRows() int64
	NumCols() int64
}

type bow struct {
	array.Record
}

func NewBow(series ...Series) (Bow, error) {
	if len(series) == 0 {
		return nil, errors.New("bow: using a bow required at less one arrow")
	}

	record, err := newRecordFromSeries(series...)
	if err != nil {
		return nil, err
	}

	return &bow{
		Record: record,
	}, nil
}

func (b *bow) PrintRows() {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)

	// Print col names on buffer
	var header []string
	for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
		header = append(header, fmt.Sprintf("%v\t", b.Schema().Field(colIndex).Name))
	}
	_, err := fmt.Fprintln(w, strings.Join(header, ""))
	if err != nil {
		panic(err)
	}

	// Print each row on buffer
	rowChan := b.RowMapIter()
	for row := range rowChan {
		var ss []string
		for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
			ss = append(ss, fmt.Sprintf("%v\t", row[b.Schema().Field(colIndex).Name]))
		}
		_, err := fmt.Fprintln(w, strings.Join(ss, ""))
		if err != nil {
			panic(err)
		}
	}

	// Flush buffer and format lines along the way
	if err := w.Flush(); err != nil {
		panic(err)
	}
}

func (b *bow) RowMapIter() chan map[string]interface{} {
	mapChan := make(chan map[string]interface{})
	go b.rowMapIter(mapChan)
	return mapChan
}

func (b *bow) rowMapIter(mapChan chan map[string]interface{}) {
	defer close(mapChan)

	if b.NumRows() <= 0 {
		return
	}

	for rowIndex := 0; rowIndex < int(b.NumRows()); rowIndex++ {
		m := map[string]interface{}{}
		for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
			switch b.Schema().Field(colIndex).Type.ID() {
			case arrow.FLOAT64:
				vd := array.NewFloat64Data(b.Column(colIndex).Data())
				if vd.IsValid(rowIndex) {
					m[b.Schema().Field(colIndex).Name] = vd.Value(rowIndex)
				}
			case arrow.INT64:
				vd := array.NewInt64Data(b.Column(colIndex).Data())
				if vd.IsValid(rowIndex) {
					m[b.Schema().Field(colIndex).Name] = vd.Value(rowIndex)
				}
			case arrow.BOOL:
				vd := array.NewBooleanData(b.Column(colIndex).Data())
				if vd.IsValid(rowIndex) {
					m[b.Schema().Field(colIndex).Name] = vd.Value(rowIndex)
				}
			default:
				panic(fmt.Sprintf("bow: unhandled type %s",
					reflect.TypeOf(b.Schema().Field(colIndex).Type.Name())))
			}
		}
		mapChan <- m
	}
}

func (b *bow) NewSlice(i, j int64) Bow {
	return &bow{
		Record: b.Record.NewSlice(i,j),
	}
}
