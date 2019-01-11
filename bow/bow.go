package bow

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
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
	// tabs will be replaced by two spaces by formater
	w.Init(os.Stdout, 0, 4, 2, ' ', 0)

	// format any line (header or row)
	formatRow := func(getCellStr func(colIndex int) string) {
		var cells []string
		for colIndex := 0; colIndex < int(b.NumCols()); colIndex++ {
			cells = append(cells, fmt.Sprintf("%v", getCellStr(colIndex)))
		}
		_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
		if err != nil {
			panic(err)
		}
	}

	// Print col names on buffer
	formatRow(func(colIndex int) string {
		return b.Schema().Field(colIndex).Name
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
		Record: b.Record.NewSlice(i, j),
	}
}
