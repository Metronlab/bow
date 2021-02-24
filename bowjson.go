package bow

import (
	"encoding/json"
	"fmt"
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
	rec := jsonRecord{
		Data: make([]map[string]interface{}, 0, b.NumRows()),
	}
	for _, col := range b.Schema().Fields() {
		rec.Schema.Fields = append(rec.Schema.Fields, jsonField{
			Name: col.Name,
			Type: col.Type.Name(),
		})
	}

	for row := range b.RowMapIter() {
		if len(row) == 0 {
			continue
		}
		rec.Data = append(rec.Data, row)
	}

	return json.Marshal(rec)
}

func (b *bow) UnmarshalJSON(data []byte) error {
	rec := jsonRecord{}
	if err := json.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("bow.UnmarshalJSON: %w", err)
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

	if rec.Data == nil {
		return fmt.Errorf("bow.UnmarshalJSON: empty rows")
	}

	series := make([]Series, len(rec.Schema.Fields))
	for i, colSchema := range rec.Schema.Fields {
		t := newTypeFromArrowName(colSchema.Type)
		buf, err := NewBufferFromInterfacesIter(t,
			len(rec.Data),
			func() chan interface{} {
				cellsChan := make(chan interface{})
				go func(cellsChan chan interface{}, colName string) {
					for _, row := range rec.Data {
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
			return fmt.Errorf("bow.UnmarshalJSON: %w", err)
		}
		series[i] = NewSeries(colSchema.Name, t, buf.Value, buf.Valid)
	}

	tmpBow, err := NewBow(series...)
	if err != nil {
		return fmt.Errorf("bow.UnmarshalJSON: %w", err)
	}
	b.Record = tmpBow.(*bow).Record

	return nil
}
