This directory contains a workflow from the original HEFT paper. It has been updated to demonstrate some of the functions of TopSim. In conjunction with `test/data/heft_single_observation_simulation.json`, we create a workflow and machine config based on the Topcuoglu et al. 2002 HEFT graph.

- Computation costs reflect the total number of FLOPs required across a whole workflow (hence their size)
- All time values are in seconds:
    * Durations are converted based on 'timestep' by dividing by the multiplier (e.g. 600 seconds becomes 10 minutes)
    * Rates are converted based on `timestep` by multiplier by the multiplier (e.g. 10GB/s becomes 600GB/minute). 
- It is not possible to represent the original HEFT graph with a single set of heterogeneous machines (the values used were arbitrary). Hence the makespan for this graph is 98 (minutes). 