Planning
========

.. currentmodule:: topsim.algorithms.planning

The :py:class:`~Planning` abstract base class defines the interface for
all planning models. A planning model generates a
:py:class:`~topsim.core.planner.WorkflowPlan` for a given observation,
producing the task list and execution order that the scheduler will
later execute dynamically.

Key method
----------

``generate_plan(clock, cluster, buffer, observation, max_ingest, task_data, edge_data)``

    Returns a :py:class:`~topsim.core.planner.WorkflowPlan`.

Built-in implementations
-------------------------

:py:class:`~topsim.user.plan.batch_planning.BatchPlanning`
    Topologically sorts the workflow DAG. Uses no external scheduling
    library. Suitable for batch-scheduling workflows.

:py:class:`~topsim.user.plan.static_planning.SHADOWPlanning`
    Uses the SHADOW library for static scheduling. Supports HEFT,
    PHEFT, and FCFS algorithms.

.. seealso::

    :ref:`user-defined` for guidance on implementing custom planning
    algorithms.
