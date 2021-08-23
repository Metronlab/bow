package bow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
)

func (b *bow) RenameCol(colIndex int, newName string) (Bow, error) {
	if colIndex >= b.NumCols() {
		return nil, fmt.Errorf("bow.RenameCol: column index out of bound")
	}

	if newName == "" {
		return nil, fmt.Errorf("bow.RenameCol: newName cannot be empty")
	}

	var colNames []string
	var arrays []array.Interface
	for i, col := range b.Columns() {
		if i == colIndex {
			colNames = append(colNames, newName)
		} else {
			colNames = append(colNames, b.ColumnName(i))
		}
		arrays = append(arrays, col)
	}

	rec, err := newRecordFromArrays(b.Metadata(), colNames, arrays)
	if err != nil {
		return nil, fmt.Errorf("bow.RenameCol: %w", err)
	}

	return &bow{Record: rec}, nil
}
