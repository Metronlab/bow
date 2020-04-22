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
