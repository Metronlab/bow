package bow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/flight"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v7/parquet"
	"github.com/apache/arrow/go/v7/parquet/file"
	"github.com/apache/arrow/go/v7/parquet/metadata"
	"github.com/apache/arrow/go/v7/parquet/pqarrow"
	"github.com/apache/arrow/go/v7/parquet/schema"
	"io"
	"os"
	"strings"
)

func NewBowFromParquet2(path string, verbose bool) (Bow, error) {
	fmt.Printf("READ PARQUET FILE: %s\n", path)
	parquetFile, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet2 open: %w", err)
	}
	defer parquetFile.Close()

	by, err := json.MarshalIndent(parquetFile.MetaData(), "", "\t")
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet2 marshal: %w", err)
	}
	fmt.Printf("PARQUET FILE META\n%s\n", string(by))

	props := pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: parquetFile.NumRows(),
	}

	manifest, err := NewSchemaManifest(
		parquetFile.MetaData().Schema,
		parquetFile.MetaData().KeyValueMetadata(), &props)
	if err != nil {
		return nil, err
	}
	fmt.Printf("MANIFEST: %+v\n", manifest)

	fileReader, err := pqarrow.NewFileReader(parquetFile, props, memory.NewGoAllocator())
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet2 NewFileReader: %w", err)
	}

	sc, err := fileReader.Schema()
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet2 Schema: %w", err)
	}
	fmt.Printf("SCHEMA: %+v\n", sc.Metadata())

	rr, err := fileReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("bow.NewBowFromParquet2 GetRecordReader: %w", err)
	}

	fmt.Printf("SCHEMA: %+v\n", rr.Schema().Metadata())

	rec, err := rr.Read()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("bow.NewBowFromParquet2 Read: %w", err)
	}
	if rec == nil {
		return NewBowEmpty(), nil
	}

	/*
		for _, f := range rec.Schema().Fields() {
			if getBowTypeFromArrowType(f.Type) == Unknown {
				return nil, fmt.Errorf("unsupported type: %s", f.Type.Name())
			}
		}
	*/

	return &bow{Record: rec, parquetMeta: parquetFile.MetaData()}, nil
}

type schemaTree struct {
	manifest *SchemaManifest

	schema *schema.Schema
	props  *pqarrow.ArrowReadProperties
}

func (s schemaTree) LinkParent(child, parent *pqarrow.SchemaField) {
	s.manifest.ChildToParent[child] = parent
}

// SchemaManifest represents a full manifest for mapping a Parquet schema
// to an arrow Schema.
type SchemaManifest struct {
	descr        *schema.Schema
	OriginSchema *arrow.Schema
	SchemaMeta   *arrow.Metadata

	ColIndexToField map[int]*pqarrow.SchemaField
	ChildToParent   map[*pqarrow.SchemaField]*pqarrow.SchemaField
	Fields          []pqarrow.SchemaField
}

func NewSchemaManifest(sc *schema.Schema, meta metadata.KeyValueMetadata, props *pqarrow.ArrowReadProperties) (*SchemaManifest, error) {
	var ctx schemaTree
	ctx.manifest = &SchemaManifest{
		ColIndexToField: make(map[int]*pqarrow.SchemaField),
		ChildToParent:   make(map[*pqarrow.SchemaField]*pqarrow.SchemaField),
		descr:           sc,
		Fields:          make([]pqarrow.SchemaField, sc.Root().NumFields()),
	}
	ctx.props = props
	ctx.schema = sc

	var err error
	ctx.manifest.OriginSchema, err = getOriginSchema(meta, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("getOriginSchema: %w", err)
	}

	return ctx.manifest, nil
}

func getOriginSchema(meta metadata.KeyValueMetadata, mem memory.Allocator) (*arrow.Schema, error) {
	if meta == nil {
		return nil, nil
	}

	const arrowSchemaKey = "ARROW:schema"
	serialized := meta.FindValue(arrowSchemaKey)
	if serialized == nil {
		return nil, nil
	}

	fmt.Printf("SERIALIZED:%s\n", *serialized)
	decoded, err := base64.StdEncoding.DecodeString(*serialized)
	if err != nil {
		return nil, fmt.Errorf("base64 DecodeString: %w", err)
	}

	return flight.DeserializeSchema(decoded, mem)
}

func (b *bow) WriteParquet2(path string, verbose bool) error {
	fmt.Printf("WRITE PARQUET FILE: %s\n", path)
	if b.NumCols() == 0 {
		return fmt.Errorf("bow.WriteParquet2: no columns")
	}

	if !strings.HasSuffix(path, ".parquet") {
		path += ".parquet"
	}

	w, err := os.Create(path)
	if err != nil {
		return err
	}

	fWriter, err := pqarrow.NewFileWriter(b.Schema(), w,
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fWriter.Close()

	if err = fWriter.Write(b.Record); err != nil {
		return err
	}

	if verbose {
		fmt.Printf(
			"bow.WriteParquet2: %s successfully written: %d bytes\n",
			path, fWriter.RowGroupTotalBytesWritten())
	}

	return nil
}
