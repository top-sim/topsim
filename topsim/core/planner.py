import sys
import logging
import copy

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
    """

    def __init__(self, env, algorithm, cluster, delay_model=None):
        self.env = env
        self.cluster = cluster
        # self.envconfig = envconfig
        self.algorithm = algorithm
        self.delay_model = delay_model

    def run(self, observation):
        # wfid = observation.name
        observation.plan = self.plan(observation.name, observation.workflow,
                                     self.algorithm)
        yield self.env.timeout(0)

    def plan(self, name, workflow, algorithm):
        workflow = ShadowWorkflow(workflow)
        available_resources = self.cluster_to_shadow_format()
        workflow_env = ShadowEnvironment(available_resources, dictionary=True)
        workflow.add_environment(workflow_env)
        plan = WorkflowPlan(name, workflow, algorithm, self.env,
                            self.delay_model)
        return plan

    def cluster_to_shadow_format(self):
        """
        Given the cluster, select from the available resources to allocate
        and create a dictionary in the format required for shadow.
        :return: dictionary of machine requirements
        """
        available_resources = self.cluster.resources['available']
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
    WorkflowPlans are used within the Planner, SchedulerA Actors and Cluster
    Resource. They are higher-level than the shadow library representation,
    as they are a storage component of scheduled tasks, rather than directly
    representing the DAG nature of the workflow. This is why the tasks are
    stored in queues.
    """

    def __init__(self, observation, workflow, algorithm, env, delay_model):
        self.id = observation
        if algorithm is 'heft':
            self.solution = shadow_heft(workflow)
        elif algorithm is 'pheft':
            self.solution = shadow_pheft(workflow)
        elif algorithm is 'fcfs':
            self.solution = shadow_fcfs(workflow)
        else:
            raise RuntimeError("Other algorithms are not supported")
        logger.debug(
            "Solution makespan for {0} is {1}".format(
                algorithm, self.solution.makespan
            )
        )

        self.eft = self.solution
        self.tasks = []
        for task in self.solution.task_allocations:
            allocation = self.solution.task_allocations.get(task)
            tid = self._create_observation_task_id(task.tid, env)
            dm = copy.copy(delay_model)
            taskobj = Task(tid, dm)
            taskobj.est = allocation.ast
            taskobj.eft = allocation.aft
            taskobj.duration = taskobj.eft - taskobj.est
            taskobj.machine_id = allocation.machine
            taskobj.flops = task.flops_demand
            taskobj.pred = [
                self._create_observation_task_id(x.tid, env) for x in list(
                    workflow.graph.predecessors(task)
                )
            ]
            self.tasks.append(taskobj)
        self.tasks.sort(key=lambda x: x.est)
        self.exec_order = self.solution.execution_order
        self.start_time = None
        self.priority = 0
        self.status = WorkflowStatus.UNSCHEDULED

    def _create_observation_task_id(self, tid, env):
        return self.id + '_' + str(env.now) + '_' + str(tid)

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority

    def __gt__(self, other):
        return self.priority > other.priority

    def is_finished(self):
        return self.status == WorkflowStatus.FINISHED
