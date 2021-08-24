package bow

var (
	stdDataSet, _ = NewGenBow(
		OptionGenCols(len(allType)),
		OptionGenDataTypes(allType))
)
