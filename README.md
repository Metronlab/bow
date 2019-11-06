# BOW

This project is an experimental go native dataframe based on [apache arrow](https://godoc.org/github.com/apache/arrow/go/arrow).

It's not production ready and will still justify a lot of refactoring before the main Bow 
interface become stable.

Discussion is open with gonum community about golang dataframe future design, 
this project will eventually be merge / replaced by 
[dframe](https://github.com/sbinet-gonum/exp/tree/dframe-proposal/dframe).
Original design that shape dframe project design comes from 
[Wes McKinney's paper](https://docs.google.com/document/d/1XHe_j87n2VHGzEbnLe786GHbbcbrzbjgG8D0IXWAeHg/edit#)
on arrow dataframe in C++.

## improvements
- handle metadata
- handle more types
- use go gen to compensate lack of generics
- add data pipelines primitives
- add conversion, scaling... execution possibilities
- add column renaming, selection..schema modification possibilities
- refactor to wrap a storage (arrow record wrapping) and execution (func exe and aggregation) layers in a Bow facade to allow user using other package logic from bow, not from another package - design in process
- documentation completion to answer open source project requirements
- deploy ci runners
