package bow

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

func (b *bow) String() string {
	if b.NumCols() == 0 {
		return ""
	}
	w := new(tabwriter.Writer)
	writer := new(strings.Builder)
	// tabs will be replaced by two spaces by formatter
	w.Init(writer, 0, 4, 2, ' ', 0)

	// format any line (header or row)
	formatRow := func(getCellStr func(colIndex int) string) {
		var cells []string
		for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
			cells = append(cells, fmt.Sprintf("%v", getCellStr(colIndex)))
		}
		_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
		if err != nil {
			panic(err)
		}
	}

	// Print col names on buffer
	formatRow(func(colIndex int) string {
		return fmt.Sprintf("%s:%v", b.Schema().Field(colIndex).Name, b.GetColType(colIndex))
	})

	// Print each row on buffer
	rowChan := b.RowMapIter()
	for row := range rowChan {
		formatRow(func(colIndex int) string {
			return fmt.Sprintf("%v", row[b.Schema().Field(colIndex).Name])
		})
	}

	_, err := fmt.Fprintf(w, "metadata: %+v\n", b.GetMetadata())
	if err != nil {
		panic(err)
	}

	// Flush buffer and format lines along the way
	if err := w.Flush(); err != nil {
		panic(err)
	}

	return writer.String()
}
