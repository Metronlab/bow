package bow

import "fmt"

// AppendBows attempts to append bows with equal schemas.
// Different schemas will lead to undefined behavior.
// Resulting metadata is copied from the first bow.
func AppendBows(bows ...Bow) (Bow, error) {
	if len(bows) == 0 {
		return nil, nil
	}

	if len(bows) == 1 {
		return bows[0], nil
	}

	refBow := bows[0]
	numRows := 0
	for _, b := range bows {
		if !b.Schema().Equal(refBow.Schema()) {
			return nil,
				fmt.Errorf("bow.AppendBow: schema mismatch: got both\n%v\nand\n%v",
					refBow.Schema(), b.Schema())
		}

		if b.Metadata().String() != refBow.Metadata().String() {
			return nil,
				fmt.Errorf("bow.AppendBow: schema Metadata mismatch: got both\n%v\nand\n%v",
					refBow.Metadata(), b.Metadata())
		}

		numRows += b.NumRows()
	}

	seriesSlice := make([]Series, refBow.NumCols())
	bufSlice := make([]Buffer, refBow.NumCols())
	for colIndex := 0; colIndex < refBow.NumCols(); colIndex++ {
		var rowOffset int
		typ := refBow.ColumnType(colIndex)
		name := refBow.ColumnName(colIndex)
		bufSlice[colIndex] = NewBuffer(numRows, typ, true)
		for _, b := range bows {
			for ri := 0; ri < b.NumRows(); ri++ {
				bufSlice[colIndex].SetOrDrop(ri+rowOffset, b.GetValue(colIndex, ri))
			}
			rowOffset += b.NumRows()
		}

		seriesSlice[colIndex] = NewSeries(name, typ, bufSlice[colIndex].Value, bufSlice[colIndex].Valid)
	}

	return NewBowWithMetadata(
		Metadata{refBow.Metadata().Metadata},
		seriesSlice...)
}
