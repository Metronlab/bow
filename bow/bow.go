package bow

import (
	"encoding/json"
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

	Equal(Bow) bool
	// todo: design and rethink:
	// Merge(bows ...Bow) (Bow, error)

	SetMarshalJSONRowBased(rowOriented bool)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error

	// Surcharged on Record:
	NewSlice(i, j int64) Bow

	// Exposed from Record:
	Release()
	Retain()

	NumRows() int64
	NumCols() int64
}

type bow struct {
	marshalJSONRowBased bool
	array.Record
}

func NewBow(series ...Series) (Bow, error) {
	if len(series) == 0 {
		return &bow{}, nil
	}

	record, err := newRecordFromSeries(series...)
	if err != nil {
		return nil, err
	}

	return &bow{
		Record: record,
	}, nil
}

func NewBowFromColumnBasedInterfaces(columnsNames []string, types []Type, columns [][]interface{}) (bobow Bow, err error) {
	if len(columnsNames) != len(columns) {
		return nil, errors.New("bow: columnsNames name and values doesn't match")
	}

	if types != nil && len(columnsNames) != len(types) {
		return nil, errors.New("bow: columnsNames name and types doesn't match")
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
		return nil, errors.New("bow: empty rows")
	}
	columnBasedRows := make([][]interface{}, len(columnsNames))
	for column := range columnsNames {
		columnBasedRows[column] = make([]interface{}, len(rows))
	}
	for rowI, row := range rows {
		if len(columnsNames) < len(row) {
			return nil, errors.New("bow: mismatch between columnsNames names and row len")
		}
		for colI  := range columnsNames {
			columnBasedRows[colI][rowI] = row[colI]
		}
	}
	return NewBowFromColumnBasedInterfaces(columnsNames, types, columnBasedRows)
}

func (b *bow) PrintRows() {
	if b.Record == nil {
		return
	}
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

	if b.Record == nil || b.NumRows() <= 0 {
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
					b.Schema().Field(colIndex).Type.Name()))
			}
		}
		mapChan <- m
	}
}

func (b *bow) Equal(b2 Bow) bool {
	b1Chan := b.RowMapIter()
	b2Chan := b2.RowMapIter()
	for {
		i1, ok1 := <- b1Chan
		i2, ok2 := <- b2Chan
		for (i1 == nil || len(i1) == 0) && ok1 {
			i1, ok1 = <- b1Chan
		}
		for (i2 == nil || len(i2) == 0) && ok2 {
			i2, ok2 = <- b2Chan
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

func (b *bow) SetMarshalJSONRowBased(rowOriented bool) {
	b.marshalJSONRowBased = rowOriented
}

func (b *bow) MarshalJSON() ([]byte, error) {
	if b.marshalJSONRowBased {
		rowBased := struct {
			ColumnsTypes map[string]string        `json:"columnsTypes"`
			Rows         []map[string]interface{} `json:"rows"`
		}{ColumnsTypes: map[string]string{}}
		for row := range b.RowMapIter() {
			if len(row) == 0 {
				continue
			}
			rowBased.Rows = append(rowBased.Rows, row)
		}
		for _, col := range b.Schema().Fields() {
			rowBased.ColumnsTypes[col.Name] = col.Type.Name()
		}
		return json.Marshal(rowBased)
	} else {
		panic("bow: column based json marshaller not implemented")
	}
}

func (b* bow) UnmarshalJSON(data []byte) error {
	jsonBow := struct {
		// Columns specifics
		Columns map[string]interface{} `json:"columns"`

		// Rows specifics
		ColumnsTypes map[string]string        `json:"columnsTypes"`
		Rows         []map[string]interface{} `json:"rows"`
	}{}

	if err := json.Unmarshal(data, &jsonBow); err != nil {
		return err
	}
	if jsonBow.Columns != nil {
		panic("bow: column based json unMarshaller not implemented")
	} else {
		b.SetMarshalJSONRowBased(true)
		series := make([]Series, len(jsonBow.ColumnsTypes))
		i := 0
		for colName, ArrowTypeName := range jsonBow.ColumnsTypes {
			t, err := getTypeFromArrowType(ArrowTypeName)
			if err != nil {
				return err
			}
			buf, err := NewBufferFromInterfacesIter(t, len(jsonBow.Rows), func()chan interface{} {
				cellsChan := make(chan interface{})
				go func (cellsChan chan interface{}, colName string){
					for _, row := range jsonBow.Rows {
						val, ok := row[colName]
						if !ok {
							cellsChan <- nil
						} else {
							_, ok = val.(float64)
							if t == Int64 && ok {
								val = int64(val.(float64))
							}
							cellsChan <- val
						}
					}
					close(cellsChan)
				}(cellsChan, colName)
				return cellsChan
			}())
			if err != nil {
				return err
			}
			series[i] = NewSeries(colName, t, buf.Value, buf.Valid)
			i++
		}
		tmpBow, err := NewBow(series...)
		if err != nil {
			return err
		}
		b.Record = tmpBow.(*bow).Record
	}

	return nil
}

func (b *bow) NewSlice(i, j int64) Bow {
	return &bow{
		Record: b.Record.NewSlice(i, j),
	}
}
