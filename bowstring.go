package bow

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// String returns a formatted representation of the Bow.
func (b *bow) String() string {
	if b.NumCols() == 0 {
		return ""
	}

	w := new(tabwriter.Writer)
	writer := new(strings.Builder)
	// tabs will be replaced by two spaces by formatter
	w.Init(writer, 0, 4, 2, ' ', 0)

	var cells []string
	for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
		cells = append(cells, fmt.Sprintf(
			"%v", fmt.Sprintf(
				"%s:%v", b.Schema().Field(colIndex).Name, b.ColumnType(colIndex))))
	}
	_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
	if err != nil {
		panic(err)
	}

	for row := range b.GetRowsChan() {
		cells = []string{}
		for colIndex := 0; colIndex < b.NumCols(); colIndex++ {
			cells = append(cells, fmt.Sprintf("%v", row[b.Schema().Field(colIndex).Name]))
		}
		if _, err = fmt.Fprintln(w, strings.Join(cells, "\t")); err != nil {
			panic(err)
		}
	}

	if b.Metadata().Len() > 0 {
		if _, err = fmt.Fprintf(w, "metadata: %+v\n", b.Metadata()); err != nil {
			panic(err)
		}
	}

	if err = w.Flush(); err != nil {
		panic(err)
	}

	return writer.String()
}
