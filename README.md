# 1D7 analysis at Gloo

## Synopsis

Provides functions and scripts for 1D7 analysis at Gloo. Uses
include identifying 1d7s, calculating cohort retention rate data,
and clustering users by their early platform actions.

## Motivation

The original 1D7 analysis was done in a single Jupyter notebook, with many
choices hard-coded in and lots of ad-hoc variable names. Thus the analysis was
unreadable, inflexible, and difficult to reproduce. 
This package abstracts the major steps into documented R functions, allowing 
us to produce a readable and reproducible analysis.

## Installation

```R
install.packages('devtools')
devtools::install_github('johnchower/oneD7')
```

## Structure
`R` contains function definitions.

`inst/scripts` contains the scripts that execute the analysis.
`inst/scripts/initial_correlation.r` establishes the correlation 
beetween initial platform action distribution and long-term retention.
