package bow

import "fmt"

func AppendBows(bows ...Bow) (Bow, error) {
	if len(bows) == 0 {
		return nil, nil
	}
	if len(bows) == 1 {
		return bows[0], nil
	}
	refBow := bows[0]
	refSchema := refBow.Schema()
	var err error
	var numRows int
	for _, b := range bows {
		schema := b.Schema()
		if !schema.Equal(refSchema) {
			return nil,
				fmt.Errorf("bow.AppendBow: schema mismatch: got both\n%v\nand\n%v",
					refSchema, schema)
		}

		if schema.Metadata().String() != refSchema.Metadata().String() {
			return nil,
				fmt.Errorf("bow.AppendBow: schema Metadata mismatch: got both\n%v\nand\n%v",
					refSchema.Metadata(), schema.Metadata())
		}

		numRows += b.NumRows()
	}

	seriesSlice := make([]Series, refBow.NumCols())
	bufSlice := make([]Buffer, refBow.NumCols())
	var name string
	for ci := 0; ci < refBow.NumCols(); ci++ {
		var rowOffset int
		typ := refBow.GetType(ci)
		name, err = refBow.GetName(ci)
		if err != nil {
			return nil, err
		}
		bufSlice[ci] = NewBuffer(numRows, typ, true)
		for _, b := range bows {
			for ri := 0; ri < b.NumRows(); ri++ {
				bufSlice[ci].SetOrDrop(ri+rowOffset, b.GetValue(ci, ri))
			}
			rowOffset += b.NumRows()
		}

		seriesSlice[ci] = NewSeries(name, typ, bufSlice[ci].Value, bufSlice[ci].Valid)
	}

	return NewBowWithMetadata(
		Metadata{refSchema.Metadata()},
		seriesSlice...)
}
