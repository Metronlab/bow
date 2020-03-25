package bow

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

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
					valids := getValids(prevArray.NullBitmapBytes())
					for rowIndex := 0; rowIndex < prevArray.Len(); rowIndex++ {
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
					valids := getValids(prevArray.NullBitmapBytes())
					for rowIndex := 0; rowIndex < newArray.Len(); rowIndex++ {
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

var bitMask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}

// bitIsSet returns true if the bit at index i in buf is set (1).
func bitIsSet(buf []byte, i int) bool { return (buf[uint(i)/8] & bitMask[byte(i)%8]) != 0 }

func getValids(bytes []byte) []bool {
	valids := make([]bool, len(bytes))

	for i := 0; i < len(bytes); i++ {
		if bitIsSet(bytes, i) {
			valids[i] = true
		} else {
			valids[i] = false
		}
	}
	return valids
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
	return NewBow2(filledSeries...)
}
