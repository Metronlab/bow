package bow

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func ExampleNewBow() {
	b, err := NewBow(
		NewSeries("col1", []int64{1, 2, 3, 4}, nil),
		NewSeries("col2", []float64{1.1, 2.2, 3.3, 4}, []bool{true, false, true, true}),
		NewSeries("col3", []bool{true, false, true, false}, []bool{true, false, true, true}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(b)
	// Output:
	// col1:int64  col2:float64  col3:bool
	// 1           1.1           true
	// 2           <nil>         <nil>
	// 3           3.3           true
	// 4           4             false
}

func ExampleNewBowFromColBasedInterfaces() {
	colNames := []string{"time", "value", "valueFromJSON"}
	colTypes := make([]Type, len(colNames))
	colTypes[0] = Int64
	colBasedData := [][]interface{}{
		{1, 1.2, json.Number("3")},
		{1, json.Number("1.2"), 3},
		{json.Number("1.1"), 2, 1.3},
	}

	b, err := NewBowFromColBasedInterfaces(colNames, colTypes, colBasedData)
	if err != nil {
		panic(err)
	}

	fmt.Println(b)
	// Output:
	// time:int64  value:int64  valueFromJSON:float64
	// 1           1            1.1
	// 1           <nil>        2
	// 3           3            1.3
}

func ExampleNewBowFromRowBasedInterfaces() {
	colNames := []string{"time", "value", "valueFromJSON"}
	colTypes := []Type{Int64, Int64, Float64}
	rowBasedData := [][]interface{}{
		{1, 1, json.Number("1.1")},
		{1.2, json.Number("1.2"), 2},
		{json.Number("3"), 3, 1.3},
	}

	b, err := NewBowFromRowBasedInterfaces(colNames, colTypes, rowBasedData)
	if err != nil {
		panic(err)
	}

	fmt.Println(b)
	// Output:
	// time:int64  value:int64  valueFromJSON:float64
	// 1           1            1.1
	// 1           <nil>        2
	// 3           3            1.3
}

func ExampleBow_MarshalJSON() {
	colNames := []string{"time", "value", "valueFromJSON"}
	colTypes := make([]Type, len(colNames))
	colTypes[0] = Int64
	colBasedData := [][]interface{}{
		{1, 1.2, json.Number("3")},
		{1, json.Number("1.2"), 3},
		{json.Number("1.1"), 2, 1.3},
	}

	b, err := NewBowFromColBasedInterfaces(colNames, colTypes, colBasedData)
	if err != nil {
		panic(err)
	}

	js, err := b.MarshalJSON()
	if err != nil {
		panic(err)
	}

	// pretty print json
	var out bytes.Buffer
	if err = json.Indent(&out, js, "", "\t"); err != nil {
		panic(err)
	}

	fmt.Println(out.String())
	// Output:
	// {
	// 	"schema": {
	// 		"fields": [
	// 			{
	// 				"name": "time",
	// 				"type": "int64"
	// 			},
	// 			{
	// 				"name": "value",
	// 				"type": "int64"
	// 			},
	// 			{
	// 				"name": "valueFromJSON",
	// 				"type": "float64"
	// 			}
	// 		]
	// 	},
	// 	"data": [
	// 		{
	// 			"time": 1,
	// 			"value": 1,
	// 			"valueFromJSON": 1.1
	// 		},
	// 		{
	// 			"time": 1,
	// 			"valueFromJSON": 2
	// 		},
	// 		{
	// 			"time": 3,
	// 			"value": 3,
	// 			"valueFromJSON": 1.3
	// 		}
	// 	]
	// }
}
