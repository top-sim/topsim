.. _model_overview:

What is a TopSim simulation?
====================================

A TopSim simulation involves the interactions of _actors_ that participate in the management of workflows that 'process' the data ingested from a user-defined Instrument.

As the motivation is a Telescope, a minimum viable simulation problem may be described as meeting the following requirements:

1. I have an observation that runs for a period of time on a Telescope
2. The observation generates data at a certain (fixed) rate.
3. There is a computing infrastructure that supports data ingest and real-time computing during this process.
4. There is a workflow that describeds tasks for data processing post-observation
5. There is a scheduling infrastructre that allows for tasks to be mapped to the computing infrastructure in a way that may be specified by the user.



The following actors are modelled within TopSim, each participating in some way in the execution of workflows:

* :py:class:`~instrument.Instrument`
* :py:class:`~scheduler.Scheduler`
* :py:class:`~cluster.Cluster`
* :py:class:`~buffer.Buffer`
* :py:class:`~monitor.Monitor`


Runtime Design
--------------

Each actor within TOpSim has a `run()` function. These are generators
themselves, yielding a timeout per-timestep, and run at the beginning of the
simulation using an env.process() call. The point of the 'run()' method for
actors is to set up a continual call-back loop, where each actor processes
the current simulation state at each timestep to see if the state has changed
and they need to act.

******
Actors
******

Buffer
======

The buffer is a core-component of the Operations Simlator model. In this
scenario, we split the buffer into two components, the Hot and Cold buffer,
as this mirrors the use case for the SKA. Additionally, it makes sense to
separate the real-time streaming timeline from the
post-observation/instrumentation workflow processing.

The Buffer operates within it's own process loop, like the Telescope,
Scheduler, and Cluster. The pseudo-code for this is as follows:

Interacting with the Buffer
----------------------------
Buffer interaction happen primarily through the Scheduler and Cluster.

The cluster and scheduler both have access to the Buffer object, and are able
to invoke processes on it.




