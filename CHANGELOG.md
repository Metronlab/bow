v0.12.0 [2021-06-10]
-------------------

- Apache Parquet file read/write support
- Add Schema Metadata support
- Add golangci-lint usage

v0.11.0 [2021-05-17]
-------------------

- Add new bow.Diff function
- Depreciate Difference aggregation

v0.10.0 [2021-05-11]
-------------------

- Rolling:
    - improved code readability
    - aggregation/fill: it is now possible to pass a previous row option to the rolling to enable the correct interpolation of the first row of its first window, in the case of missing window start row

v0.9.0 [2021-03-24]
-------------------

- General:
    - Fix typos
    - Improve robustness and code clarity of functions IsColEmpty, IsColSorted and FillLinear with better error management
    - Remove unused variables
    - Remove bow.marshalJSONRowBased
    
- Bow Generator:
    - Improve randomness of values
    - Added support for String and Bool data types
    - New ColNames and DataTypes options for more flexibility
    - Improve user experience with better error management

- Benchmarks improvements:
    - Added new test cases
    - Added usage of benchstat on the CircleCI pipeline to compare benchmark results with master branch

- New Functions:
    - NewValuesFromJSON

- Bug fix:
  - Rolling inclusive window with duplicated indexes now correctly iterate keeping windowing integrity

v0.8.0 [2021-02-12]
-------------------

- New functions:
    - IsEmpty
    - FindFirst
    - IsSupported
    - GetReturnType
- Adding strong typing support
- Refactoring Bow's logic to return a valid schema instead of nil when no data is found
- Fixing tests

v0.7.3 [2021-01-12]
-------------------

- New functions:
    - NewBowEmpty
    - NewBowFromColNames
    - EncodeBowToJSONBody
    - DecodeJSONRespToBow
- New aggregation tests
- Minor code refactoring

v0.7.2 [2020-09-14]
-------------------

### Bugfixes
- OuterJoin: support of bow without rows returning correct schema

v0.7.1 [2020-08-03]
-------------------

### Features
- Add SortByCol method to sort a bow by a column name

v0.6.2 [2020-06-02]
-------------------

#### Bugfixes
- InnerJoin

v0.6.1 [2020-04-22]
-------------------

#### Bugfixes
- bump arrow to apache-arrow-0.17.0

#### Known issues
arrow now allow several column with same name introducing new panics in bow if the case happen.
[corresponding issue](https://github.com/Metronlab/bow/issues/12)

v0.6.0 [2020-04-22]
-------------------

#### Features
- Add Fill functions for missing data interpolation
- Add OuterJoin method
- Refactor InnerJoin method
- Add new CI with CircleCI
- Refactor the sub package bow to have the main functionalities available in the root module

#### How to migrate to v0.6.0
It is necessary to replace the library import path from github.com/Metronlab/bow/bow to github.com/Metronlab/bow

0.2.0 [2019-02-19]
-------------------

#### Features

- Depreciate method to print in favor to a stringer interface
- Innerjoin based on column name for now, we'll have to let more liberty over the join later on
- Map based indexes for join optimisation (divide time per 5 on simple short sample)

#### Bugfixes

- Fix empty series that make the code segfault in arrow, can now have empty dataframe with schema/record set.


0.1.0 [2019-02-01]
-------------------

#### Features

- Row based json encoding and decoding
- New Bow fron row and column based [][]interfaces{}
- Method to print

0.0.0 [2019-01-11]
-------------------

#### Features

- Initial Release
- Simple dataframe with type and series based on apache arrow
