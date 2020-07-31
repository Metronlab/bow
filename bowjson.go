package bow

import (
	"encoding/json"
	"errors"
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
