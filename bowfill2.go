package bow

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

type Series2 struct {
	Name  string
	Array array.Interface
}

func (b *bow) FillPrevious2(colNames ...string) (Bow, error) {
	toFill, err := colsToFill(b, colNames)
	if err != nil {
		return nil, err
	}

	seriesChannel := make(chan Series2, b.NumCols())
	for colIndex, col := range b.Schema().Fields() {
		go func(colIndex int, colName string) {
			typ := b.GetType(colIndex)
			var newArray array.Interface
			prevData := b.Record.Column(colIndex).Data()
			if toFill[colIndex] {
				switch typ {
				case Int64:
					prevArray := array.NewInt64Data(prevData)
					values := prevArray.Int64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							prevInt, prevRow := b.GetPreviousInt64(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevInt
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewInt64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				case Float64:
					prevArray := array.NewFloat64Data(prevData)
					values := prevArray.Float64Values()
					valids := getValids(prevArray.NullBitmapBytes(), len(values))
					for rowIndex := 0; rowIndex < len(valids); rowIndex++ {
						if !valids[rowIndex] {
							prevInt, prevRow := b.GetPreviousFloat64(colIndex, rowIndex-1)
							if prevRow > -1 {
								values[rowIndex] = prevInt
								valids[rowIndex] = true
							}
						}
					}
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					b := array.NewFloat64Builder(pool)
					b.AppendValues(values, valids)
					newArray = b.NewArray()
				}
			} else {
				newArray = array.MakeFromData(prevData)
			}
			seriesChannel <- Series2{
				Name:  colName,
				Array: newArray,
			}
		}(colIndex, col.Name)
	}
	return newBowFromSeries2Channel(b, seriesChannel)
}

func newBowFromSeries2Channel(b *bow, seriesChannel chan Series2) (Bow, error) {
	seriesCounter := 0
	filledSeries := make([]Series2, b.NumCols())
	for s := range seriesChannel {
		for colIndex, col := range b.Schema().Fields() {
			if s.Name == col.Name {
				filledSeries[colIndex] = s
				seriesCounter++
				if seriesCounter == b.NumCols() {
					close(seriesChannel)
				}
			}
		}
	}
	return newBow2(filledSeries...)
}

func newBow2(series ...Series2) (Bow, error) {
	record, err := newRecordFromSeries2(series...)
	if err != nil {
		return nil, err
	}

	return &bow{
		Record: record,
	}, nil
}

func newRecordFromSeries2(series ...Series2) (array.Record, error) {
	if len(series) == 0 {
		return nil, nil
	}

	var fields []arrow.Field
	var cols []array.Interface
	var nrows int
	for _, s := range series {
		if s.Name == "" {
			return nil, errors.New("bow: empty series name")
		}
		field := arrow.Field{Name: s.Name}
		if getTypeFromArrowType(s.Array.DataType()) == Unknown {
			return nil, fmt.Errorf("bow: unhandled type: %s", s.Array.DataType().Name())
		}
		field.Type = s.Array.DataType()
		fields = append(fields, field)
		cols = append(cols, s.Array)
		nrows = s.Array.Len()
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, int64(nrows)), nil
}
