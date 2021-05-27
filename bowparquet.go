package bow

import (
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"log"
	"os"
	"strings"
)

var ParquetToBowType = map[parquet.Type]Type{
	parquet.Type_BOOLEAN:    Bool,
	parquet.Type_DOUBLE:     Float64,
	parquet.Type_INT64:      Int64,
	parquet.Type_BYTE_ARRAY: String,
}

func ParquetFileRead(file string) (Bow, error) {
	r, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer func(r *os.File) {
		err := r.Close()
		if err != nil {
			panic(err)
		}
	}(r)

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		return nil, err
	}

	log.Printf("reading file %s", file)

	series := make([]Series, len(fr.Columns()))
	for i, col := range fr.Columns() {
		log.Printf("Col:%+v type:%+v", col, col.Type())
		buf := NewBuffer(int(fr.NumRows()), ParquetToBowType[*col.Type()], true)
		series[i] = NewSeries(col.Name(), ParquetToBowType[*col.Type()], buf.Value, buf.Valid)
	}

	return NewBow(series...)
}

func (b *bow) ParquetFileWrite(filename string) error {
	if !strings.HasSuffix(filename, ".parquet") {
		filename += ".parquet"
	}
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			panic(err)
		}
	}(f)

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("bow"),
	)

	for i := range b.Schema().Fields() {
		var store *goparquet.ColumnStore
		switch b.GetType(i) {
		case Int64:
			store, err = goparquet.NewInt64Store(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
			if err != nil {
				panic(err)
			}
		}

		c := goparquet.NewDataColumn(store, parquet.FieldRepetitionType_OPTIONAL)
		err = fw.AddColumn("", c)
		if err != nil {
			panic(err)
		}
	}

	inputData := []struct {
		ID   int
		City string
		Pop  int
	}{
		{ID: 1, City: "Berlin", Pop: 3520031},
		{ID: 2, City: "Hamburg", Pop: 1787408},
		{ID: 3, City: "Munich", Pop: 1450381},
		{ID: 4, City: "Cologne", Pop: 1060582},
		{ID: 5, City: "Frankfurt", Pop: 732688},
	}

	for _, input := range inputData {
		if err := fw.AddData(map[string]interface{}{
			"id":         int64(input.ID),
			"city":       []byte(input.City),
			"population": int64(input.Pop),
		}); err != nil {
			log.Fatalf("Failed to add input %v to parquet file: %v", input, err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet file writer failed: %v", err)
	}

	return nil
}
