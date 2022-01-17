User-defined operations in TOpSim
====================================



TopSims development is motivated by the evaluation of the workflow scheduling
procedures that are proposed for the Square Kilometre Array Science Data
Processor. For this reason the evaluation of different scheduling policies is
a defining feature of the simulation framework.

TopSim separates allocation policies into two categories:

* Reservation-based allocation, such as SLURM or typical batch-processing models
* Free allocation, which is the model that popular heuristics such as HEFT
are built upon.
