---
title: "Release notes for `PFUPipeline2`"
output: html_document
---


* Added new download script for Joao Santos (IST).
* Remove dependency on `Recca::verify_SUT_Energy_Balance()`,
  which is now deprecated.


## PFUPipeline2 v0.2.2 (2025-09-26)

* Added a report script for Avery Sugg (University of Texas)
* Organized code between _targets.R and _pl_setup.R.
* Added a report script for Baptiste Andrieu query
  from June 2025.
* Fixed the calc_Ef_to_Xloss_exiobase function as there was a mistake in code.
* Added a report script for Joao Santos query from May 2025.


## PFUPipeline2 v0.2.1 (2025-05-27) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15531641.svg)](https://doi.org/10.5281/zenodo.15531641)

* Beginning to deal with multiple versions.
* Responded to enhancements in PFUPipelineTools.


## PFUPipeline2 v0.2.0 (2025-05-15) 

* Moving from PFUPipeline to PFUPipeline2. 
* Controlling in-database compression is now possible with 
  variable `compress_data` in `_pl_setup.R`.
