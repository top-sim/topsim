import sys
import logging
from enum import Enum
# sys.path.append(path.abspath('../../shadow'))

from topsim.core.task import Task

from shadow.models.workflow import Workflow as ShadowWorkflow
from shadow.models.environment import Environment as ShadowEnvironment
from shadow.algorithms.heuristic import heft as shadow_heft

logger = logging.getLogger(__name__)


# BUFFER_OFFSET = config_data.buffer_offset
# from core.telescope import Observation


class Planner:
    """
    The Planner is our interface with static scheduling algorithms. It provides
    an interface to other libraries and selects the library based on the provided
    algorithms based to the _init_. Currently, the SHADOW library is the only
    library that the Planner is aligned with; this may change in the future.
    """

    def __init__(self, env, algorithm, cluster):
        self.env = env
        self.cluster = cluster
        # self.envconfig = envconfig
        self.algorithm = algorithm

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
        plan = WorkflowPlan(name, workflow, algorithm, self.env)
        return plan

    def cluster_to_shadow_format(self):
        """
        Given the cluster, select from the available resources to allocate
        and create a dictionary in the format required for shadow.
        :return: dictionary of machine requirements
        """
        sdict = {}
        # "flops": 84,
        # "rates": 10
        # "costs": 0.7
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


# for machine in available_resources:


class WorkflowStatus(int, Enum):
    UNSCHEDULED = 1
    SCHEDULED = 2
    ON_TIME = 3
    DELAYED = 4
    FINISHED = 5


class WorkflowPlan:
    """
    WorkflowPlans are used within the Planner, SchedulerA Actors and Cluster Resource. They are higher-level than the
    shadow library representation, as they are a storage component of scheduled tasks, rather than directly representing
    the DAG nature of the workflow. This is why the tasks are stored in queues.
    """

    def __init__(self, wid, workflow, algorithm, env):
        self.id = wid
        if algorithm is 'heft':
            self.solution = shadow_heft(workflow)
        else:
            sys.exit("Other algorithms are not supported")

        # DO Task execution things here
        taskid = 0
        ast = 1
        aft = 2
        self.tasks = []
        task_order = []

        # The solution object is now how we get information on allocatiosn
        # from SHADOW

        for task in self.solution.task_allocations:
            allocation = self.solution.task_allocations.get(task)
            taskobj = Task(task.tid, env)
            taskobj.est = allocation.ast
            taskobj.eft = allocation.aft
            taskobj.duration = taskobj.eft - taskobj.est
            taskobj.machine_id = allocation.machine
            taskobj.flops = task.flops_demand
            taskobj.pred = list(workflow.graph.predecessors(task))
            self.tasks.append(taskobj)
        self.tasks.sort(key=lambda x: x.est)
        self.exec_order = self.solution.execution_order
        self.start_time = None
        self.priority = 0
        self.status = WorkflowStatus.UNSCHEDULED
        self.delayed = None

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority

    def __gt__(self, other):
        return self.priority > other.priority

    def is_finished(self):
        return self.status == WorkflowStatus.FINISHED
