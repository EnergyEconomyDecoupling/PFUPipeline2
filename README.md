
<!-- *********** -->
<!-- Note: README.md is generated from README.Rmd.   -->
<!-- Be sure to edit README.Rmd and generate the README.md file by Cmd/Ctl-shift-K -->
<!-- *********** -->
<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/PFUPipeline2)](https://cran.r-project.org/package=PFUPipeline2)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![Project Status: WIP – Initial development is in progress, but there
has not yet been a stable, usable release suitable for the
public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
<!-- [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5228375.svg)](https://doi.org/10.5281/zenodo.5228375) -->
<!-- badges: end -->

# PFUPipeline2

## Statement of need

The CL-PFU database uses many sources of input data spanning hundreds of
data files. When creating the CL-PFU database, matrices are created
according to the
[PSUT](https://www.sciencedirect.com/science/article/pii/S0306261918308298?via%3Dihub)
framework, final energy data are extended to the useful stage, and
energy conversion chains are converted to exergy quantifications. Many
calculations (aggregations, efficiencies, etc.) are needed, too. A way
to specify all calculation steps is needed. Furthermore, the
calculations take many hours to complete, so what is done should stay
done during debugging. A calculation pipeline is needed, one that
clearly identifies all steps in database creation and one that can be
resumed where needed.

This package (`PFUPipeline2`) provides functions to create several
products of the CL-PFU database. The primary objectives of
`PFUPipeline2` are

- to create data frames of **RUVY** matrices in
  [matsindf](https://MatthewHeun.github.io/matsindf/) format and
- to calculate further data products, such as aggregations and
  efficiencies.

A [targets](https://docs.ropensci.org/targets/) pipeline provides
helpful dependency management for all calculations.

## History

`PFUPipeline2` is the successor to `PFUPipeline` and `PFUAggPipeline`,
which are now superceded. `PFUPipeline` and `PFUAggPipeline` created
flat files (`pins`) of output data. In contrast, `PFUPipeline2` provides
a computational pipeline that uses a
[PostgreSQL](https://www.postgresql.org) database for data storage.
There are so many differences flat file storage and database storage
(i.e., between `PFUPipeline`/`PFUAggPipeline` and `PFUPipeline2`) that
it made sense to start over.

## Installation

You can install the development version of `PFUPipeline2` from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("EnergyEconomyDecoupling/PFUPipeline2")
```

## Quick start

At the RStudio console, type

``` r
library(targets)              # to load the targets package   
tar_visnetwork()              # to see a directed acyclic graph of the calculations that will take place   
tar_make()                    # to execute the calculations (or `workers = 8`, if you have enough cores)
```

## Accessing targets

`targets::tar_read(<<target>>)` pulls the value of a target out of the
`targets` cache. (`<<target>>` should be an unquoted symbol such as
`Specified`.)

## Fresh start

`targets::tar_destroy()` invalidates the `targets` cache and forces
reanalysis of everything. Reanalyzing everything may take a while.

## More Information

For information about the `targets` package, see the [targets
manual](https://books.ropensci.org/targets/).

For documentation on the `PFUPipeline2` package, see
<https://EnergyEconomyDecoupling.github.io/PFUPipeline2/>.

## Acknowledgements

The CL-PFU database is supported by an EPSRC
[fellowship](https://environment.leeds.ac.uk/energy-climate-change-mitigation/dir-record/research-projects/1773/epsrc-fellowship-applying-thermodynamic-laws-to-the-energy-gdp-decoupling-problem)
awarded to Paul Brockway of Leeds University. A goal of the fellowship
is building a world database of country-specific primary, final, and
useful exergy for 1960–2019.
