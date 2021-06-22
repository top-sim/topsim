import sys
import logging
import copy

import networkx as nx

from enum import Enum

from topsim.core.task import Task

from shadow.models.workflow import Workflow as ShadowWorkflow
from shadow.models.environment import Environment as ShadowEnvironment
from shadow.algorithms.heuristic import heft as shadow_heft
from shadow.algorithms.heuristic import pheft as shadow_pheft
from shadow.algorithms.heuristic import fcfs as shadow_fcfs

logger = logging.getLogger(__name__)


class WorkflowStatus(int, Enum):
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

    def __init__(self, env, algorithm, cluster, delay_model=None):
        self.env = env
        self.cluster = cluster
        # self.envconfig = envconfig
        self.algorithm = algorithm
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
        # wfid = observation.name
        # if observation.ast is None:
        #     raise RuntimeError(
        #         f'Observation AST must be updated before plan'
        #     )
        observation.plan = self.plan(
            observation, observation.workflow, self.algorithm, buffer
        )

        yield self.env.timeout(0)

    def plan(self, observation, workflow, algorithm, buffer):
        """

        Parameters
        ----------
        observation
        workflow
        algorithm
        buffer

        Returns
        -------

        """
        if observation.ast is None:
            raise RuntimeError(
                f'Observation AST must be updated before plan'
            )

        workflow = ShadowWorkflow(workflow)
        available_resources = self.cluster_to_shadow_format()
        workflow_env = ShadowEnvironment(available_resources, dictionary=True)
        workflow.add_environment(workflow_env)
        plan = WorkflowPlan(observation, workflow, algorithm, self.env,
                            self.delay_model, buffer)
        return plan

    def cluster_to_shadow_format(self):
        """
        Given the cluster, select from the available resources to allocate
        and create a dictionary in the format required for shadow.
        :return: dictionary of machine requirements
        """

        # TODO we have reverted to the entire list of machines; can we
        #  improve this moving forward?
        # TODO entire machines
        available_resources = list(self.cluster.dmachine.values())
        dictionary = {
            "system": {
                "resources": None,
                "bandwidth": self.cluster.system_bandwidth
            }
        }
        resources = {}
        for m in available_resources:
            resources[m.id] = {
                "flops": m.cpu,
                "rates": m.bandwidth,
                "io": m.disk,
                "memory": m.memory
            }
        dictionary['system']['resources'] = resources

        return dictionary


class WorkflowPlan:
    """
    WorkflowPlans are used within the Planner, Scheduler, and Cluster. 
    They are higher-level than the shadow library representation,
    as they are a storage component of scheduled tasks, rather than directly
    representing the DAG nature of the workflow. This is why the tasks are
    stored in queues.
    """

    def __init__(self, observation, workflow, algorithm, env, delay_model,
                 buffer):
        self.id = observation.name
        if algorithm is 'heft':
            self.solution = shadow_heft(workflow)
        elif algorithm is 'pheft':
            self.solution = shadow_pheft(workflow)
        elif algorithm is 'fcfs':
            self.solution = shadow_fcfs(workflow)
        elif algorithm is 'batch':
            self.solution = None
        else:
            raise RuntimeError("Other algorithms are not supported")
        logger.debug(
            "Solution makespan for {0} is {1}".format(
                algorithm, self.solution.makespan
            )
        )

        if algorithm is not 'batch':
            self.est = self._calc_workflow_est(observation, buffer)
            self.eft = self.solution.makespan
            self.tasks = []
            for task in self.solution.task_allocations:
                allocation = self.solution.task_allocations.get(task)
                tid = self._create_observation_task_id(task.tid, env)
                dm = copy.copy(delay_model)
                predecessors = [
                    self._create_observation_task_id(x.tid, env) for x in list(
                        workflow.graph.predecessors(task)
                    )
                ]
                taskobj = Task(
                    tid,
                    allocation.ast,
                    allocation.aft,
                    allocation.machine,
                    predecessors,
                    task.flops_demand, 0, 0,
                    dm
                )
                self.tasks.append(taskobj)
            self.tasks.sort(key=lambda x: x.est)
            self.exec_order = self.solution.execution_order



    def _generate_topological_plan(self):
        """
        This is used if we do not use a planning schedule, and instead are
        operating on a batch-processing model. The batch processing model
        means that we take up all resources assigned to the workflow.
        Returns
        -------

        """
        pass

    def _create_observation_task_id(self, tid, env):
        return self.id + '_' + str(env.now) + '_' + str(tid)

    def _create_task_from_nxgraph(self, task, solution, delay,workflow):
        """
        Helper function to simplify task generation in the WorkflowPlan
        constructor

        Parameters
        ----------
        task
        solution
        delay
        workflow

        Returns
        -------
        wf_task :
            WorkflowPlan Task with instantiated values based on a scheduled
            task graph.

        """

    def _calc_workflow_est(self,observation, buffer):
        """
        Calculate the estimated start time of the workflow based on data
        transfer delays post-observation

        Parameters
        ----------
        Returns
        -------

        """

        storage = buffer.buffer_storage_summary()
        size = observation.duration * observation.ingest_data_rate
        hot_to_cold_time = int(size/storage['coldbuffer']['data_rate'])
        est = observation.duration + hot_to_cold_time
        return est

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority

    def __gt__(self, other):
        return self.priority > other.priority

    def is_finished(self):
        return self.status == WorkflowStatus.FINISHED
