.. _user-defined:

Overview
========

TopSim's development is motivated by evaluating workflow scheduling
policies for large-scale radio telescopes like the SKA. The framework
separates allocation policies into two categories:

* **Reservation-based allocation** (e.g. SLURM-style batch processing)
* **Free allocation** (e.g. HEFT-style dynamic scheduling)

Users can implement custom algorithms for both planning and scheduling
by extending the abstract base classes provided by TopSim.

Writing a custom Planning algorithm
====================================

Planning algorithms generate a :py:class:`~topsim.core.planner.WorkflowPlan`
for an observation. Extend
:py:class:`~topsim.algorithms.planning.Planning` and implement:

* ``generate_plan(clock, cluster, buffer, observation, max_ingest)``
* ``to_string()``
* ``to_df()``

Example skeleton:

.. code-block:: python

    from topsim.algorithms.planning import Planning
    from topsim.core.planner import WorkflowPlan, WorkflowStatus

    class MyPlanning(Planning):
        def __init__(self, algorithm='my_algo', delay_model=None):
            super().__init__(algorithm, delay_model)

        def generate_plan(self, clock, cluster, buffer,
                          observation, max_ingest,
                          task_data=False, edge_data=True):
            # Build tasks, determine execution order,
            # return a WorkflowPlan
            return WorkflowPlan(...)

        def to_string(self):
            return 'MyPlanning'

        def to_df(self):
            pass

Two built-in planning implementations are provided:

* :py:class:`~topsim.user.plan.batch_planning.BatchPlanning` -
  Topologically sorts the workflow DAG for batch scheduling.
* :py:class:`~topsim.user.plan.static_planning.SHADOWPlanning` -
  Uses the SHADOW library (HEFT, PHEFT, FCFS).

Writing a custom Scheduling algorithm
======================================

Scheduling algorithms determine which tasks to allocate to which machines
at each timestep. Extend
:py:class:`~topsim.algorithms.scheduling.Scheduling` and implement:

* ``run(cluster, planner, clock, workflow_plan, existing_schedule, task_pool, **kwargs)``
* ``to_string()``
* ``to_df()``

Example skeleton:

.. code-block:: python

    from topsim.algorithms.scheduling import Scheduling

    class MyScheduling(Scheduling):
        def __init__(self):
            super().__init__()
            self.name = 'MyScheduling'

        def run(self, cluster, planner, clock, workflow_plan,
                existing_schedule, task_pool, **kwargs):
            # Return (schedule, workflow_plan, task_pool)
            return existing_schedule, workflow_plan, task_pool

        def to_string(self):
            return self.name

        def to_df(self):
            pass

Four built-in scheduling implementations are provided:

* :py:class:`~topsim.user.schedule.batch_allocation.BatchProcessing` -
  Early-binding batch scheduling.
* :py:class:`~topsim.user.schedule.dynamic_plan.DynamicSchedulingFromPlan` -
  Follows a pre-computed static plan.
* :py:class:`~topsim.user.schedule.greedy.GreedySchedulingFromPlan` -
  Greedy re-allocation based on runtime resource availability.
* :py:class:`~topsim.user.schedule.queue_allocation.QueueProcessing` -
  Queue-model scheduling without a static plan.

Writing a custom Instrument
============================

To create a custom instrument, extend
:py:class:`~topsim.core.instrument.Instrument` and implement:

* ``run()`` - The per-timestep process loop.
* ``to_df()`` - Return per-timestep state as a DataFrame.

See :py:class:`~topsim.user.telescope.Telescope` for a reference
implementation that manages antenna array allocation and coordinates
with the Scheduler for ingest capacity.

Putting it all together
========================

Use your custom algorithms with the ``topsim experiment`` CLI:

.. code-block:: bash

    topsim experiment \\
        -i config.json \\
        -p mymodule.MyPlanning \\
        -s mymodule.MyScheduling \\
        -d EdgeData \\
        -o ./results \\
        run

Or via the :py:class:`~topsim.utils.experiment.Experiment` class:

.. code-block:: python

    from topsim.utils.experiment import Experiment

    exp = Experiment(
        configuration='config.json',
        alloc_combinations=[(MyPlanning, MyScheduling)],
        data_combinations=[{'use_task_data': False, 'use_edge_data': True}],
        output='./results'
    )
    exp.run()

If you need full control (e.g. a custom Instrument), use the
:py:class:`~topsim.core.simulation.Simulation` class directly:

.. code-block:: python

    from topsim.core.simulation import Simulation

    simulation = Simulation(
        env=env, config='config.json',
        instrument=MyInstrument,
        planning_model=MyPlanning('my_algo'),
        scheduling=MyScheduling()
    )
    simulation.start()
