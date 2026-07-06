"""
Algorithm presents the abstract base class for any Scheduling algorithm.
"""

from abc import ABC, abstractmethod

from topsim.core.cluster import Cluster
from topsim.core.planner import Planner, WorkflowPlan


class Scheduling(ABC):
    """
    Abstract base class for all scheduling algorithms.

    Scheduling algorithms determine which tasks to allocate to which
    machines at each simulation timestep. They are used by the
    :py:class:`~topsim.core.scheduler.Scheduler` actor during dynamic
    task allocation.

    An 'incorrect' algorithm (one that allocates to an occupied machine
    or double-schedules a task) will raise :py:class:`RuntimeError`.

    Attributes
    ----------
    name : str
    ingest_requirements : int
        Number of resources allocated for ingest pipelines.

    Notes
    -----
    The constants ``LOW_REALTIME_RESOURCES``, ``MID_REALTIME_RESOURCES``,
    ``LOW_MAX_RESOURCES``, and ``MID_MAX_RESOURCES`` are derived from the
    SDP parametric model and are used for resource provisioning heuristics.
    """

    LOW_REALTIME_RESOURCES = 164
    MID_REALTIME_RESOURCES = 281

    LOW_MAX_RESOURCES = 896
    MID_MAX_RESOURCES = 786

    def __init__(self):
        self.name = "AbstractAlgorithm"
        self.ingest_requirements = 0

    @abstractmethod
    def to_string(self):
        """
        Return the string name of this scheduling implementation.
        """
        pass

    @abstractmethod
    def run(self,
            cluster: Cluster,
            planner: Planner,
            clock, plan: WorkflowPlan,
            schedule,
            task_pool,
            **kwargs):
        """
        Generate a set of task-machine allocations for the current timestep.

        Parameters
        ----------
        cluster : Cluster
            The Cluster actor (for resource state).
        planner : Planner
            The Planner actor (to generate new plans if needed).
        clock : int
            Current simulation time.
        plan : WorkflowPlan
            The existing workflow plan for the observation.
        schedule : dict
            The existing schedule of {Task: Machine} allocations.
        task_pool : set of Task
            Tasks that are ready for allocation.
        **kwargs
            Additional keyword arguments (e.g. ``observation``).

        Returns
        -------
        tuple
            (schedule, workflow_plan, task_pool) where schedule is the
            updated {Task: Machine} mapping, and task_pool is the updated
            set of ready tasks.
        """
        pass

    @abstractmethod
    def to_df(self):
        """
        Produce a DataFrame with the current state of the algorithm.

        Returns
        -------
        pandas.DataFrame
        """
