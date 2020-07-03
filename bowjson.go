package bow

import (
	"encoding/json"
	"errors"
)

func (b *bow) SetMarshalJSONRowBased(rowOriented bool) {
	b.marshalJSONRowBased = rowOriented
}

type jsonColSchema struct {
	Name string
	Type string
}

type jsonRecord struct {
	schema struct {
		fields []jsonColSchema
	}
	data []map[string]interface{}
}

func (b *bow) MarshalJSON() ([]byte, error) {
	if !b.marshalJSONRowBased {
		// it will be handled natively by arrow, today (24 oct 2019) still in arrow's internal packages
		panic("bow: column based json marshaller not implemented")
	}
	rowBased := jsonRecord{}
	for _, col := range b.Schema().Fields() {
		rowBased.schema.fields = append(rowBased.schema.fields, jsonColSchema{
			Name: col.Name,
			Type: col.Type.Name(),
		})
	}
	for row := range b.RowMapIter() {
		if len(row) == 0 {
			continue
		}
		rowBased.data = append(rowBased.data, row)
	}
	return json.Marshal(rowBased)
}

func (b *bow) UnmarshalJSON(data []byte) error {
	jsonBow := jsonRecord{}
	if err := json.Unmarshal(data, &jsonBow); err != nil {
		return err
	}
	if jsonBow.data != nil {
		series := make([]Series, len(jsonBow.schema.fields))
		i := 0
		for _, colSchema := range jsonBow.schema.fields {
			t := newTypeFromArrowName(colSchema.Type)
			buf, err := NewBufferFromInterfacesIter(t, len(jsonBow.data), func() chan interface{} {
				cellsChan := make(chan interface{})
				go func(cellsChan chan interface{}, colName string) {
					for _, row := range jsonBow.data {
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
