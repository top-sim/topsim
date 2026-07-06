.. _model_overview:

What is a TopSim simulation?
============================

A TopSim simulation models an end-to-end run-through of a mid-term
observation plan for a (radio) telescope. The simulation involves the
interaction of *actors* that participate in the management of workflows
that process the data ingested from an instrument.

The minimum viable simulation problem meets these requirements:

1. An observation runs for a period of time on a telescope.
2. The observation generates data at a fixed rate.
3. There is a computing infrastructure that supports data ingest and
   real-time computing.
4. There is a workflow describing tasks for post-observation data
   processing.
5. There is a scheduling infrastructure that maps tasks to computing
   resources.

Actors
=======

The following actors are modelled within TopSim:

* :py:class:`~topsim.user.telescope.Telescope` (or a custom
  :py:class:`~topsim.core.instrument.Instrument` implementation)
* :py:class:`~topsim.core.scheduler.Scheduler`
* :py:class:`~topsim.core.cluster.Cluster`
* :py:class:`~topsim.core.buffer.Buffer`
* :py:class:`~topsim.core.planner.Planner`
* :py:class:`~topsim.core.monitor.Monitor`

Runtime design
--------------

Each actor has a ``run()`` method that yields a ``simpy.Timeout`` per
timestep. These are started at the beginning of the simulation using
``env.process()``. The ``run()`` method sets up a continual callback
loop where each actor checks the current simulation state and acts
accordingly.

Data flow
---------

::

    Instrument ──► Scheduler ──► Buffer ──► Cluster
        │              │            │           │
        │              │            │           ▼
        │              │            │        Tasks
        │              │            │
        ▼              ▼            ▼
     Observations   WorkflowPlans  Hot/Cold storage

1. The **Instrument** manages observations and checks instrument
   capacity (antenna arrays).
2. When ready, the **Scheduler** checks ingest capacity (buffer and
   cluster availability).
3. The **Planner** generates a :py:class:`~topsim.core.planner.WorkflowPlan`
   for the observation's workflow DAG.
4. The **Buffer** manages data flow: observations ingest into the
   HotBuffer, then data is transferred to the ColdBuffer.
5. The **Scheduler** dynamically allocates tasks from the plan to
   machines in the **Cluster**.
6. The **Monitor** records per-timestep state and events from all
   actors.

Buffer
------

The Buffer is split into two components: the **HotBuffer** (streaming
ingest) and **ColdBuffer** (post-observation storage). This mirrors
the SKA use case, where real-time streaming data is separated from
post-processing workflow data.

The Buffer runs its own process loop. Key interactions:

* The Instrument writes observation data to the HotBuffer during ingest.
* Data is transferred from HotBuffer to ColdBuffer at the ColdBuffer's
  ``max_data_rate``.
* The Scheduler retrieves observations from the Buffer for workflow
  processing.
* The Cluster processes tasks that read data from processed observations.

For more detail on each actor, see the :ref:`reference/index` section.
