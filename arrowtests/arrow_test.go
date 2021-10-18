package arrowtests

import "github.com/apache/arrow/go/arrow/array"

func ExamplePrintRecordColumns() {
	_, rec := NewTSRecord()
	defer rec.Release()

	PrintRecordColumns(rec)

	// Output:
	//column[0] "time": [1 2 3 4]
	//column[1] "value": [7 8 (null) 10]
	//column[2] "quality": [42 42 41 42]
}

func ExamplePrintRecordRows() {
	s, rec := NewTSRecord()
	defer rec.Release()

	PrintRecordRows(s, []array.Record{rec})

	// Output:
	//time: 1 , value: 7 , quality: 42
	//time: 2 , value: 8 , quality: 42
	//time: 3 , value: <nil> , quality: 41
	//time: 4 , value: 10 , quality: 42
}
