package bow

import (
	"github.com/apache/arrow/go/arrow/array"
)

//Bow is a wrapper of apache arrow array record.
// It was not implemented as a facade shadowing arrow
// in order to expose low lvl arrow decisions to bow users
// while arrow is in beta
type Bow interface {
	//RowMapIter() chan GenericMap
	//RowMapIterApply(func(GenericMap) error) error
	//
	//UnmarshalJSON([]byte) error
	//MarshalJSON() ([]byte, error)

	// todo: design and rethink:
	// Merge(bows ...Bow) (Bow, error)

	// Surcharged on Record:
	NewSlice(i, j int64) Bow

	// Exposed from Record:
	Release()
	Retain()

	NumRows() int64
	NumCols() int64
}

type bow struct {
	array.Record
}

func NewBow(gms []GenericMap) (Bow, error) {
	return makeBowFromMaps(gms)
}

//NewBowFromStream await a consistant flow of maps and assert a key type as soon as possible.
//if a key is alway empty it return an array of null floats for this key and set
//the whole col empty in metadata
//The function returns when the channel is closed.
func NewBowFromStream(cField chan map[string]interface{}) (Bow, error) {
	panic("implement me")
}

func (b *bow) NewSlice(i, j int64) Bow {
	return &bow{
		Record: b.Record.NewSlice(i,j),
	}
}
//func (b *bow) RowMapIter() chan map[string]interface{} {
//	mapChan := make(chan map[string]interface{})
//	go b.rowMapIter(mapChan)
//	return mapChan
//}
//
//func (b *bow) rowMapIter(mapChan chan map[string]interface{}) {
//	defer close(mapChan)
//
//	if b.NumRows() <= 0 {
//		return
//	}
//
//	table := array.NewTableFromRecords(b.Schema(), []array.Record{b.Record})
//	defer table.Release()
//
//	// makes a events buffer
//	events := make([]Event, table.NumRows())
//
//	// Seek schema index for event
//	timeIndex := table.Schema().FieldIndex("time")
//	if timeIndex < 0 {
//		panic(errors.New("impossible to convert record to event"))
//	}
//	valueIndex := table.Schema().FieldIndex("value")
//	if valueIndex < 0 {
//		panic(errors.New("impossible to convert record to event"))
//	}
//	qualityIndex := table.Schema().FieldIndex("quality")
//	if qualityIndex < 0 {
//		panic(errors.New("impossible to convert record to event"))
//	}
//
//	// TableReader is able to iter on a table grouping by indexes,
//	// marvellous to do calculation in parallel
//	// Underutilized in this case, for a naive implementation iteration is done 1 by 1
//	tr := array.NewTableReader(table, 1)
//	defer tr.Release()
//
//	// fill buffer with TableReader iteration
//	index := 0
//	for tr.Next() {
//		rec := tr.Record()
//
//		td := array.NewTime32Data(rec.Column(timeIndex).Data())
//		if td.IsValid(0) {
//			events[index].Time = td.Time32Values()[0]
//		}
//		vd := array.NewFloat64Data(rec.Column(valueIndex).Data())
//		if vd.IsValid(0) {
//			events[index].Value = vd.Float64Values()[0]
//		}
//		qd := array.NewInt64Data(rec.Column(qualityIndex).Data())
//		if qd.IsValid(0) {
//			events[index].quality = qd.Int64Values()[0]
//		}
//
//		index++
//	}
//}
