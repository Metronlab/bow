package bow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

func (b *bow) SetMarshalJSONRowBased(rowOriented bool) {
	b.marshalJSONRowBased = rowOriented
}

var (
	ErrUnmarshalJSON = errors.New("could not unmarshal JSON to bow")
	ErrEncodeBow     = errors.New("could not encode bow to JSON body")
	ErrDecodeJSON    = errors.New("could not decode JSON to bow")
)

type jsonField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type jsonSchema struct {
	Fields []jsonField `json:"fields"`
}

type jsonRecord struct {
	Schema jsonSchema               `json:"schema"`
	Data   []map[string]interface{} `json:"data"`
}

func (b *bow) MarshalJSON() ([]byte, error) {
	if !b.marshalJSONRowBased {
		// it will be handled natively by arrow, today (26 nov 2020) still in arrow's internal packages
		panic("bow: column based json marshaller not implemented")
	}

	rowBased := jsonRecord{}
	for _, col := range b.Schema().Fields() {
		rowBased.Schema.Fields = append(rowBased.Schema.Fields, jsonField{
			Name: col.Name,
			Type: col.Type.Name(),
		})
	}

	for row := range b.RowMapIter() {
		if len(row) == 0 {
			continue
		}
		rowBased.Data = append(rowBased.Data, row)
	}

	return json.Marshal(rowBased)
}

func UnmarshalJSON(data []byte) (Bow, error) {
	var rec jsonRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}

	if rec.Schema.Fields == nil {
		return NewBowEmpty(), nil
	}

	series := make([]Series, len(rec.Schema.Fields))
	for i, f := range rec.Schema.Fields {
		typ := newTypeFromArrowName(f.Type)
		length := len(rec.Data)
		buf, err := NewBufferFromInterfacesIter(
			typ,
			length,
			func() chan interface{} {
				cellsChan := make(chan interface{})
				go func(cellsChan chan interface{}, colName string) {
					for _, row := range rec.Data {
						val, ok := row[colName]
						if !ok {
							cellsChan <- nil
						} else {
							_, ok = val.(float64)
							if typ == Int64 && ok {
								val = int64(val.(float64))
							}
							cellsChan <- val
						}
					}
					close(cellsChan)
				}(cellsChan, f.Name)

				return cellsChan
			}(),
		)
		if err != nil {
			return nil, fmt.Errorf("%v: %v", ErrUnmarshalJSON, err)
		}
		series[i] = NewSeries(f.Name, typ, buf.Value, buf.Valid)
	}

	return NewBow(series...)
}

func EncodeBowToJSONBody(b Bow) (io.Reader, error) {
	b.SetMarshalJSONRowBased(true)
	jsonBody, err := b.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrEncodeBow, err)
	}

	return bytes.NewReader(jsonBody), nil
}

func DecodeJSONRespToBow(resp io.Reader) (Bow, error) {
	respBytes, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrDecodeJSON, err)
	}

	var rec jsonRecord
	err = json.Unmarshal(respBytes, &rec)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrDecodeJSON, err)
	}

	/*
			Convert back json_table data types to bow data types

			From pandas / io / json / _table_schema.py / as_json_table_type(x: DtypeObj) -> str:
		    This table shows the relationship between NumPy / pandas dtypes,
		    and Table Schema dtypes.
		    ==============  =================
		    Pandas type     Table Schema type
		    ==============  =================
		    int64           integer
		    float64         number
		    bool            boolean
		    datetime64[ns]  datetime
		    timedelta64[ns] duration
		    object          str
		    categorical     any
		    =============== =================
	*/

	for i, f := range rec.Schema.Fields {
		if _, ok := mapArrowDataTypeNameType[f.Type]; ok {
			continue
		}
		switch f.Type {
		case "integer":
			rec.Schema.Fields[i].Type = "int64"
		case "number":
			rec.Schema.Fields[i].Type = "float64"
		case "boolean":
			rec.Schema.Fields[i].Type = "bool"
		}
	}

	jsonRec, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrDecodeJSON, err)
	}

	outputBow, err := UnmarshalJSON(jsonRec)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrDecodeJSON, err)
	}

	return outputBow, nil
}
