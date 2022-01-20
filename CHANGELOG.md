# Changelog

From 2022-01-20, all changes will described in this file. 

This project takes inspiration from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Current official version is found in the master branch.  

Procedures for versioning: 

- Change is made on feature branch, based on bug or issue. 
- Multiple changes are then merge into a 'version' branch for further testing
- Once the testing has been established, this version is merged into master, updating the current main version of the library. 

## [Unreleased]
Outline of planned changes.

### [0.4.0]

- Refactor cluster and buffer internal dictionaries to remove `self.buffer[b]...` code. 

### [0.3.1]

Update data output functions  output to remove performance warnings from HDF5 tables

## [0.3.0] 2022-01-10
*[Retrospective versioning]*

### Changed
- Data output uses HDF5 data tables to improve data storage after simulations

### Removed
- TopSim no longer uses `Pandas` pickles to store output data. 

## [0.2.0] - 2021-08-9

### Added 

*[Retrospective versioning]*


- Planner abstract base class established for more flexibility with planning algorithms

- Batch provisioning in `Cluster`

### Changed 

- `Scheduling` algorithm Abstract base class requires `existing_schedule` paramater to 

- Documentation uses Pydata Sphinx theme 

## [0.1.0] 2021-05-13

*[Retrospective versioning]*

This version is going to correspond with the first 'public' release and results of TopSim, which was it's demonstration in the 2021 ISC-HPC conference. 

This can be found either at [this commit](https://github.com/top-sim/topsim/commit/d9f43315d83ff814ac5e4b474f9ac8eeab1c0180), or at the [2021-isc-hpc branch](https://github.com/top-sim/topsim/tree/2021-isc-hpc).

Functionality that existed included: 
- Complete simulation run using configuration files and workflows
- Scheduling algorithm integration with SHADOW library
- Data output as `panda` pickles 
- Runtime-delays with tasks
- Test coverage of >80%. 