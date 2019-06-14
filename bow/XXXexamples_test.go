package bow

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func ExampleNewBow() {
	b, err := NewBow(
		NewSeries("col1", Int64, []int64{1, 2, 3, 4}, nil),
		NewSeries("col2", Float64, []float64{1.1, 2.2, 3.3, 4}, []bool{true, false, true, true}),
		NewSeries("col3", Bool, []bool{true, false, true, false}, []bool{true, false, true, true}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Print(b)
	// output:
	// col1  col2   col3
	// 1     1.1    true
	// 2     <nil>  <nil>
	// 3     3.3    true
	// 4     4      false
}

func ExampleNewBowFromColumnBasedInterfaces() {
	columns := []string{"time", "value", "valueFromJson"}
	ts := make([]Type, len(columns))
	ts[0] = Int64
	rows := [][]interface{}{
		{1, 1.2, json.Number("3")},
		{1, json.Number("1.2"), 3},
		{json.Number("1.1"), 2, 1.3},
	}

	b, err := NewBowFromColumnBasedInterfaces(columns, ts, rows)
	if err != nil {
		panic(err)
	}
	fmt.Print(b)
	b.Release()

	// output:
	//time   value  valueFromJson
	//1      1      1.1
	//<nil>  <nil>  <nil>
	//3      3      1.3
}

func ExampleBow_MarshalJSON() {
	columns := []string{"time", "value", "valueFromJson"}
	ts := make([]Type, len(columns))
	ts[0] = Int64
	rows := [][]interface{}{
		{1, 1.2, json.Number("3")},
		{1, json.Number("1.2"), 3},
		{json.Number("1.1"), 2, 1.3},
	}

	b, err := NewBowFromColumnBasedInterfaces(columns, ts, rows)
	if err != nil {
		panic(err)
	}

	b.SetMarshalJSONRowBased(true)
	js, err := b.MarshalJSON()
	if err != nil {
		panic(err)
	}
	// pretty print json
	var out bytes.Buffer
	if err := json.Indent(&out, js, "", "\t"); err != nil {
		panic(err)
	}
	fmt.Println(string(out.Bytes()))
	//output:
	//	{
	//	"columnsTypes": {
	//		"time": "int64",
	//		"value": "int64",
	//		"valueFromJson": "float64"
	//	},
	//	"rows": [
	//		{
	//			"time": 1,
	//			"value": 1,
	//			"valueFromJson": 1.1
	//		},
	//		{
	//			"time": 3,
	//			"value": 3,
	//			"valueFromJson": 1.3
	//		}
	//	]
	//}
}
