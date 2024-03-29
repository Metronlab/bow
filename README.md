# Bow

![lint](https://github.com/Metronlab/bow/actions/workflows/golangci-lint.yml/badge.svg)
![ci](https://github.com/Metronlab/bow/actions/workflows/ci.yml/badge.svg)

Bow is meant to be an efficient data manipulation framework based on [Apache Arrow](https://arrow.apache.org/) for the Go programming language.
Inspired by [Pandas](https://pandas.pydata.org/), Bow aims to bring the last missing block required to make Go a data science ready language.

The `Bow` interface is stable and frozen, you can using it at will, all further changes will be planned for a v2.

This project have been used for years in production at [Metron](https://www.metron.energy/), 
however it's still an incomplete pet project compared to [Pandas](https://pandas.pydata.org/).
Bow is currently developed internally at Metronlab with primary concerns about timeseries.
Recently [empowill](https://www.empowill.com/) decided to contribute to confront this library to a more general purpose usage. 

We are looking for a foundation / group of people that could help send this library to the next level! 

## CONTRIBUTE
Don't hesitate to send issues and contribute to the library design.

This library is in pure Go, to contribute you just need a recent Go version installed and you can directly use `make` to validate your contribution.

- Create an issue
- Create a branch from main
- Implement and comply with the Github Actions CI
- Submit a PR

