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

type jsonColSchema struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type jsonRecord struct {
	Schema struct {
		Fields []jsonColSchema `json:"fields"`
	} `json:"schema"`
	Data []map[string]interface{} `json:"data"`
}

func (b *bow) MarshalJSON() ([]byte, error) {
	if !b.marshalJSONRowBased {
		// it will be handled natively by arrow, today (24 oct 2019) still in arrow's internal packages
		panic("bow: column based json marshaller not implemented")
	}
	rowBased := jsonRecord{}
	for _, col := range b.Schema().Fields() {
		rowBased.Schema.Fields = append(rowBased.Schema.Fields, jsonColSchema{
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

func (b *bow) UnmarshalJSON(data []byte) error {
	jsonBow := jsonRecord{}
	if err := json.Unmarshal(data, &jsonBow); err != nil {
		return err
	}
	if jsonBow.Data != nil {
		series := make([]Series, len(jsonBow.Schema.Fields))
		i := 0
		for _, colSchema := range jsonBow.Schema.Fields {
			t := newTypeFromArrowName(colSchema.Type)
			buf, err := NewBufferFromInterfacesIter(t, len(jsonBow.Data), func() chan interface{} {
				cellsChan := make(chan interface{})
				go func(cellsChan chan interface{}, colName string) {
					for _, row := range jsonBow.Data {
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
				}(cellsChan, colSchema.Name)
				return cellsChan
			}())
			if err != nil {
				return err
			}
			series[i] = NewSeries(colSchema.Name, t, buf.Value, buf.Valid)
			i++
		}
		tmpBow, err := NewBow(series...)
		if err != nil {
			return err
		}
		b.Record = tmpBow.(*bow).Record
		return nil
	}
	return errors.New("empty rows")
}

var (
	ErrInvalidNewBow    = errors.New("could not create new Bow")
	ErrEncodeInputBow   = errors.New("could not JSON encode input Bow")
	ErrReadingResponse  = errors.New("could not read POST request response")
	ErrDecodingResponse = errors.New("could not decode POST request response")
)

func EncodeBowToJSON(inputBow Bow) (io.Reader, error) {
	inputBow.SetMarshalJSONRowBased(true)
	requestBody, err := inputBow.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrEncodeInputBow, err)
	}
	fmt.Printf("requestBody:%+v\n", requestBody)

	return bytes.NewReader(requestBody), nil
}

func DecodeJSONToBow(r io.Reader, outputBow Bow) (Bow, error) {
	respBody, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrReadingResponse, err)
	}

	var jsonOutputBow jsonRecord
	err = json.Unmarshal(respBody, &jsonOutputBow)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", errors.New("unmarshal error"), err)
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

	for i, f := range jsonOutputBow.Schema.Fields {
		switch f.Type {
		case "integer":
			jsonOutputBow.Schema.Fields[i].Type = "int64"
		case "number":
			jsonOutputBow.Schema.Fields[i].Type = "float64"
		case "boolean":
			jsonOutputBow.Schema.Fields[i].Type = "bool"
		}
	}

	dec, err := json.Marshal(jsonOutputBow)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrDecodingResponse, err)
	}

	err = outputBow.UnmarshalJSON(dec)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", ErrInvalidNewBow, err)
	}

	return outputBow, nil
}
