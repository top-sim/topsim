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

    The Planner is our interface with static scheduling algorithms. It provides
    an interface to other libraries and selects the library based on the
    provided algorithms based to the _init_. Currently, the SHADOW library is
    the only library that the Planner is aligned with; this may change in the
    future.

    Attributes
    ----------
    env : simpy.Environment
        Simulation environment object

    cluster : topsim.core.cluster.Cluster
        The cluster of the simulation; necessary to pass to static scheduling
        algorithms

    algorithm : str
        The intended algorithm used for scheduling

    delay_model: topsim.core.delay.DelayModel
        The delaymodel object, to assign to each Workflow Plan task.


    """

    def __init__(self, env, algorithm, cluster, model, delay_model=None):
        self.env = env
        self.cluster = cluster
        # self.envconfig = envconfig
        self.algorithm = algorithm
        self.model = model(algorithm, cluster, delay_model)
        self.delay_model = delay_model

    def run(self, observation, buffer):
        """
        Parameters
        ----------
        observation :
            The observation for which we are generating a plan (by forming a
            schedule using the predefined static scheduling algorithm).
        buffer

        Returns
        -------

        """
        return self.model.generate_plan(self.env.now, buffer, observation)

        # yield self.env.timeout(0,plan)


class WorkflowPlan:
    """
    WorkflowPlans are used within the Planner, Scheduler, and Cluster. 
    They are higher-level than the shadow library representation,
    as they are a storage component of scheduled tasks, rather than directly
    representing the DAG nature of the workflow. This is why the tasks are
    stored in queues.
    """

    def __init__(self,
                 est, eft, tasks, exec_order,status):


        self.est = est
        self.eft = eft
        self.tasks = tasks
        self.exec_order = exec_order
        self.status = status
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


