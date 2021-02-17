.. _model_overview:

Model Overview
==============

Each Actor within TOpSim has a `run()` function. These are generators
themselves, yielding a timeout per-timestep, and run at the beginning of the
simulation using an env.process() call. The point of the 'run()' method for
actors is to set up a continual call-back loop, where each actor processes
the current simulation state at each timestep to see if the state has changed
and they need to act.

.. autoclass:: topsim.core.simulation.Simulation