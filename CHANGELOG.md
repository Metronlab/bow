0.5.0 [2019-10-27]
-------------------

#### Bugfixes

- fix previous value getter that where calling next values ones

0.5.0 [2019-10-27]
-------------------

#### Features

- string storage and aggregation

0.4.0 [2019-09-12]
-------------------

#### Features

- implement full column aggregations

0.3.0 [2019-07-22]
-------------------

#### Features

- implement aggregation and interpolations grouped by integers / only works on sorted increasing integer column 

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
