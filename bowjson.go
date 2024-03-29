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

// JSONBow is a structure representing a Bow for JSON marshaling purpose.
type JSONBow struct {
	Schema       JSONSchema               `json:"schema"`
	RowBasedData []map[string]interface{} `json:"data"`
}

// MarshalJSON returns the marshal encoding of the bow.
func (b bow) MarshalJSON() ([]byte, error) {
	return json.Marshal(NewJSONBow(&b))
}

// NewJSONBow returns a new JSONBow structure from a Bow.
func NewJSONBow(b Bow) JSONBow {
	if b == nil {
		return JSONBow{}
	}

	res := JSONBow{
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

	for row := range b.GetRowsChan() {
		if len(row) == 0 {
			continue
		}
		res.RowBasedData = append(res.RowBasedData, row)
	}

	return res
}

// UnmarshalJSON parses the JSON-encoded data and stores the result in the bow.
func (b *bow) UnmarshalJSON(data []byte) error {
	jsonB := JSONBow{}
	if err := json.Unmarshal(data, &jsonB); err != nil {
		return fmt.Errorf("json.Unmarshal: %w", err)
	}

	if err := b.NewValuesFromJSON(jsonB); err != nil {
		return fmt.Errorf("bow.NewValuesFromJSON: %w", err)
	}

	return nil

}

// NewValuesFromJSON replaces the bow arrow.Record by a new one represented by the JSONBow structure.
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

	for fieldIndex, field := range jsonB.Schema.Fields {
		if _, ok := mapArrowNameToBowTypes[field.Type]; ok {
			continue
		}
		switch field.Type {
		case "integer":
			jsonB.Schema.Fields[fieldIndex].Type = "int64"
		case "number":
			jsonB.Schema.Fields[fieldIndex].Type = "float64"
		case "boolean":
			jsonB.Schema.Fields[fieldIndex].Type = "bool"
		}
	}

	series := make([]Series, len(jsonB.Schema.Fields))

	if jsonB.RowBasedData == nil {
		for fieldIndex, field := range jsonB.Schema.Fields {
			typ := getBowTypeFromArrowName(field.Type)
			buf := NewBuffer(0, typ)
			series[fieldIndex] = NewSeriesFromBuffer(field.Name, buf)
		}

		tmpBow, err := NewBow(series...)
		if err != nil {
			return err
		}

		b.Record = tmpBow.(*bow).Record
		return nil
	}

	for fieldIndex, field := range jsonB.Schema.Fields {
		typ := getBowTypeFromArrowName(field.Type)
		buf := NewBuffer(len(jsonB.RowBasedData), typ)
		for rowIndex, row := range jsonB.RowBasedData {
			buf.SetOrDrop(rowIndex, row[field.Name])
		}

		series[fieldIndex] = NewSeriesFromBuffer(field.Name, buf)
	}

	tmpBow, err := NewBow(series...)
	if err != nil {
		return err
	}

	b.Record = tmpBow.(*bow).Record
	return nil
}
