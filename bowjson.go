package bow

import (
	"encoding/json"
	"fmt"
)

type jsonField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type JSONSchema struct {
	Fields []jsonField `json:"fields"`
}

type JSONBow struct {
	Schema       JSONSchema               `json:"schema"`
	RowBasedData []map[string]interface{} `json:"data"`
}

func (b bow) MarshalJSON() ([]byte, error) {
	return json.Marshal(NewJSONBow(&b))
}

func NewJSONBow(b Bow) (res JSONBow) {
	if b == nil {
		return
	}

	res = JSONBow{
		RowBasedData: make([]map[string]interface{}, 0, b.NumRows()),
	}
	for _, col := range b.Schema().Fields() {
		res.Schema.Fields = append(
			res.Schema.Fields,
			jsonField{
				Name: col.Name,
				Type: col.Type.Name(),
			})
	}

	for row := range b.RowMapIter() {
		if len(row) == 0 {
			continue
		}
		res.RowBasedData = append(res.RowBasedData, row)
	}
	return
}

func (b *bow) UnmarshalJSON(data []byte) error {
	jsonB := JSONBow{}
	if err := json.Unmarshal(data, &jsonB); err != nil {
		return fmt.Errorf("bow.UnmarshalJSON: %w", err)
	}

	if err := b.NewValuesFromJSON(jsonB); err != nil {
		return fmt.Errorf("bow.UnmarshalJSON: %w", err)
	}

	return nil

}

// NewValuesFromJSON replaces b values by a filled JSONBow struct
func (b *bow) NewValuesFromJSON(jsonB JSONBow) error {
	if len(jsonB.Schema.Fields) == 0 {
		b.Record = NewBowEmpty().(*bow).Record
		return nil
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

	for i, f := range jsonB.Schema.Fields {
		if _, ok := mapArrowDataTypeNameType[f.Type]; ok {
			continue
		}
		switch f.Type {
		case "integer":
			jsonB.Schema.Fields[i].Type = "int64"
		case "number":
			jsonB.Schema.Fields[i].Type = "float64"
		case "boolean":
			jsonB.Schema.Fields[i].Type = "bool"
		}
	}

	series := make([]Series, len(jsonB.Schema.Fields))

	if jsonB.RowBasedData == nil {
		for i, field := range jsonB.Schema.Fields {
			t := newTypeFromArrowName(field.Type)
			buf := NewBuffer(0, t, true)
			series[i] = NewSeries(field.Name, t, buf.Value, buf.Valid)
		}

		tmpBow, err := NewBow(series...)
		if err != nil {
			return fmt.Errorf("bow.NewValuesFromJSON: %w", err)
		}

		b.Record = tmpBow.(*bow).Record
		return nil
	}

	for i, field := range jsonB.Schema.Fields {
		t := newTypeFromArrowName(field.Type)
		buf := NewBuffer(len(jsonB.RowBasedData), t, true)
		for rowIndex, row := range jsonB.RowBasedData {
			val, ok := row[field.Name]
			if ok {
				_, ok = val.(float64)
				if t == Int64 && ok {
					val = int64(val.(float64))
				}
				buf.SetOrDrop(rowIndex, val)
			}
		}

		series[i] = NewSeries(field.Name, t, buf.Value, buf.Valid)
	}

	tmpBow, err := NewBow(series...)
	if err != nil {
		return fmt.Errorf("bow.NewValuesFromJSON: %w", err)
	}

	b.Record = tmpBow.(*bow).Record
	return nil
}
