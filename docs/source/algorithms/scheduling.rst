Scheduling
==========

.. currentmodule:: topsim.algorithms.scheduling

The :py:class:`~Scheduling` abstract base class defines the interface for
all scheduling algorithms. Scheduling algorithms determine which tasks to
allocate to which machines at each simulation timestep, based on the
current resource state, the workflow plan, and the algorithm's policy.

Key method
----------

``run(cluster, planner, clock, workflow_plan, existing_schedule, task_pool, **kwargs)``

    Returns ``(schedule, workflow_plan, task_pool)`` where ``schedule``
    is a ``{Task: Machine}`` mapping of new allocations.

Built-in implementations
-------------------------

:py:class:`~topsim.user.schedule.batch_allocation.BatchProcessing`
    Early-binding batch allocation with configurable partitioning and
    ingest awareness.

:py:class:`~topsim.user.schedule.dynamic_plan.DynamicSchedulingFromPlan`
    Follows a pre-computed static schedule, allocating tasks to their
    planned machines when predecessors are complete.

:py:class:`~topsim.user.schedule.greedy.GreedySchedulingFromPlan`
    Greedy re-allocation: if the planned machine is occupied, picks
    the first available alternative.

:py:class:`~topsim.user.schedule.queue_allocation.QueueProcessing`
    Queue-based allocation without a static plan; assigns ready tasks
    to the first available machine.

.. seealso::

    :ref:`user-defined` for guidance on implementing custom scheduling
    algorithms.
