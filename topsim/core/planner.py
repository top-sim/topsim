import sys
import logging
import copy

import networkx as nx

from enum import Enum

LOGGER = logging.getLogger(__name__)


class WorkflowStatus(int, Enum):
    """
    Status enums for a WorkflowPlan object
    """
    UNSCHEDULED = 1
    SCHEDULED = 2
    ON_TIME = 3
    DELAYED = 4
    FINISHED = 5


class Planner:
    """
    The Planner actor generates workflow plans for observations.

    It delegates plan generation to a user-specified planning model
    (an implementation of :py:class:`~topsim.algorithms.planning.Planning`),
    which may rely on static scheduling libraries such as SHADOW.

    Parameters
    ----------
    env : simpy.Environment
        Simulation environment.
    cluster : ~topsim.core.cluster.Cluster
        The Cluster actor (needed for resource information during planning).
    model : ~topsim.algorithms.planning.Planning
        The planning model instance.
    use_task_data : bool
        Whether to include per-task data in runtime calculations.
    use_edge_data : bool
        Whether to include edge data transfer times.
    delay_model : ~topsim.core.delay.DelayModel, optional
        Delay model to assign to each task in the workflow plan.
    """

    def __init__(self, env, cluster, model, use_task_data, use_edge_data, delay_model=None):
        self.env = env  #: :py:object:~`simpy.Environment` object for the
        # simulation
        self.cluster = cluster
        # self.envconfig = envconfig
        self.model = model  # algorithm, cluster, delay_model)
        self.delay_model = delay_model
        self.use_task_data = use_task_data
        self.use_edge_data = use_edge_data

    def run(self, observation, buffer, max_ingest):
        """
        Parameters
        ----------
        max_ingest
        observation :
            The observation for which we are generating a plan (by forming a
            schedule using the predefined static scheduling algorithm).
        buffer

        Returns
        -------
        core.topsim.planner.WorkflowPlan
        """
        return self.model.generate_plan(self.env.now, self.cluster, buffer,
                                        observation, max_ingest, self.use_task_data, self.use_edge_data)

        # yield self.env.timeout(0,plan)


class WorkflowPlan:
    """
    Stores the plan for a single observation's workflow.

    A WorkflowPlan contains the tasks, execution order, and graph
    representation of a workflow that has been planned (static schedule)
    for an observation. It is used by the Scheduler and Cluster during
    dynamic task allocation.

    Parameters
    ----------
    id : str
        Observation/workflow identifier.
    est : int
        Earliest start time for the workflow.
    eft : int
        Earliest finish time for the workflow.
    tasks : list of Task
        All tasks in the workflow.
    exec_order : list
        Topological execution order of tasks.
    status : WorkflowStatus
        Current status (UNSCHEDULED, SCHEDULED, FINISHED, etc.).
    max_ingest : int
        Maximum ingest resources allowed.
    graph : networkx.DiGraph, optional
        The workflow DAG with Task objects as nodes.

    Attributes
    ----------
    id : str
    tasks : list of Task
    finished_tasks : list of Task
    status : WorkflowStatus
    graph : networkx.DiGraph or None
    """

    def __init__(self, id, est, eft, tasks, exec_order, status, max_ingest,
                 graph=None):
        self.id = id
        self.est = est
        self.eft = eft
        self.tasks = tasks
        self.finished_tasks = []
        self.exec_order = exec_order
        self.status = status
        self.graph = graph
        self.min_resources = None
        self.max_resources = None
        self.priority = None

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority

    def __gt__(self, other):
        return self.priority > other.priority

    def set_workflow_status(self, status):
        """
        Update workflow status
        Parameters
        ----------
        status

        Returns
        -------

        """
        self.status = status

    def is_finished(self):
        """
        Check if the workflow has been marked as finished
        Returns
        -------

        """
        return self.status == WorkflowStatus.FINISHED

    def get_task_successors(self, task_id):
        return self.graph.successors(task_id)

    def get_task_predecessors(self, task_id):
        return self.graph.successors(task_id)

    def get_data_cost(self, task_u, task_v):
        pass
