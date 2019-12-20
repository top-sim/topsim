import sys
from os import path

# sys.path.append(path.abspath('../../shadow'))

from shadow.classes.workflow import Workflow
from shadow.classes.environment import Environment
from shadow.algorithms.heuristic import heft as shadow_heft
import test_data

# BUFFER_OFFSET = config_data.buffer_offset
# from core.telescope import Observation


class Planner(object):
	def __init__(self, env, algorithm, envconfig):
		self.env = env
		self.envconfig = envconfig
		self.wfconfig = None
		self.algorithm = algorithm
		self.wfname = None

	def run(self, observation):
		# wfid = observation.name
		self.wfname = observation.name
		self.wfconfig = observation.workflow
		observation.plan = self.plan(self.algorithm)
		yield self.env.timeout(0)

	def plan(self, algorithm):

		wf = Workflow(self.wfconfig)
		wfenv = Environment(self.envconfig)
		wf.add_environment(wfenv)
		plan = WorkflowPlan(self.wfname, None, None, None)
		if algorithm is 'heft':
			makespan = shadow_heft(wf)
			plan.makespan = makespan
		else:
			sys.exit("Other algorithms are not supported")

		plan.task_order = wf.execution_order
		plan.allocation = wf.machine_alloc
		return plan


class WorkflowPlan(object):
	"""
	WorkflowPlans are used within the Planner, SchedulerA Actors and Cluster Resource. They are higher-level than the
	shadow library representation, as they are a storage component of scheduled tasks, rather than directly representing
	the DAG nature of the workflow. This is why the tasks are stored in queues.
	"""

	def __init__(self, workflow, exec_order, allocation, makespan):
		self.id = workflow
		self.task_order = self._convert_node_to_task(exec_order)
		self.allocation = allocation
		self.makespan = makespan
		self.start_time = None
		self.priority = 0

	def _convert_node_to_task(self, task_list):
		"""
		Convert a node in the task_order list to a Task object
		:return:
		"""
		task_order = []

		return task_order


	def __lt__(self, other):
		return self.priority < other.priority

	def __eq__(self, other):
		return self.priority == other.priority

	def __gt__(self, other):
		return self.priority > other.priority


class Task(object):

	"""
	Tasks have priorities inheritted from the workflows from which they are arrived; once
	they arrive on the cluster queue, they are workflow agnositc, and are processed according to
	their priority.
	"""
	def __init__(self, tid):
		self. id = tid
		self.start = 0
		self.finish = 0
		self.flops = 0
		self.memory = None
		self.io = 0
		self.alloc = None
		self.duration = None


