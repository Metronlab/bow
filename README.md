# Bow

This project is experimental and not ready for production.
The interface and methods are still under heavy changes.

Bow is meant to be an efficient data manipulation framework based on [Apache Arrow](https://arrow.apache.org/) for the Go programming language. Inspired by [Pandas](https://pandas.pydata.org/), Bow aims to bring the last missing block required to make Golang a data science ready language.

Bow is currently developed internally at Metronlab with primary concerns about timeseries. Don't hesitate to send issues and contribute to the library design.

## Roadmap

**Data types handling**
* [x] implement string, int64, float64, bool data types
* [ ] use go gen as a palliative for the lack of generics in Go
* [ ] handle all Arrow data types

**Serialization**
* [x] expose native Arrow stringer
* [x] implement Parquet serialization
* [ ] expose native Arrow CSV
* [ ] expose native Arrow JSON
* [ ] expose native Arrow IPC

**Features**
* [x] implement windowed data aggregations
* [x] implement windowed data interpolations
* [x] implement Fill methods to handle missing data
* [x] implement InnerJoin method
* [x] implement OuterJoin method
* [x] implement Select columns method
* [x] handle Arrow Schema metadata
* [ ] implement Apply method
* [ ] implement facade for all accessible features to simplify usage
* [ ] improve Bow append method in collaboration with Arrow maintainers

**Go to v1**
* [ ] complete Go native doc
* [ ] examples for each methods
* [ ] implement package to compare Bow and Pandas performances
* [ ] API frozen, new releases won't break your code
* [ ] support dataframes with several columns having the same name
