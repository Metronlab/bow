package bow

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/compress"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
)

// NewBowFromParquet loads a parquet object from the file path, returning a new Bow
// Only value columns are used to create the new Bow.
// Argument verbose is used to print information about the file loaded.
func NewBowFromParquet(filename string, verbose bool) (Bow, error) {
	rdr, err := file.OpenParquetFile(filename, false)
	if err != nil {
		return nil, fmt.Errorf("file.OpenParquetFile: %w", err)
	}
	defer rdr.Close()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, fmt.Errorf("pqarrow.NewFileReader: %w", err)
	}

	tbl, err := arrowRdr.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("pqarrow.FileReader.ReadTable: %w", err)
	}
	defer tbl.Release()

	cols := make([]arrow.Array, tbl.NumCols())
	for i := 0; i < int(tbl.NumCols()); i++ {
		if len(tbl.Column(i).Data().Chunks()) != 1 {
			return nil, fmt.Errorf(
				"column %d has %d chunks", i, len(tbl.Column(i).Data().Chunks()))
		}
		cols[i] = tbl.Column(i).Data().Chunk(0)
	}

	rec := array.NewRecord(tbl.Schema(), cols, tbl.NumRows())
	b, err := NewBowFromRecord(rec)
	if err != nil {
		return nil, fmt.Errorf("NewBowFromRecord: %w", err)
	}

	if verbose {
		fmt.Printf(
			"bow.NewBowFromParquet: %s successfully read: %d rows\n%+v\n",
			filename, b.NumRows(), b.Schema())
	}

	return b, nil
}

// WriteParquet writes a Bow to the binary parquet format.
// Argument verbose is used to print information about the written file.
func (b *bow) WriteParquet(filename string, verbose bool) error {
	if b.NumCols() == 0 {
		return fmt.Errorf("bow has 0 columns")
	}

	if !strings.HasSuffix(filename, ".parquet") {
		filename += ".parquet"
	}

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	wr, err := pqarrow.NewFileWriter(b.Schema(), f,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(allocator)))
	if err != nil {
		return err
	}

	if err = wr.Write(b.Record); err != nil {
		return err
	}
	wr.Close()

	if verbose {
		fmt.Printf(
			"bow.WriteParquet: %s successfully written: %d rows\n",
			filename, b.NumRows())
	}

	return nil
}
