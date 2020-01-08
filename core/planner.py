import sys
from enum import Enum
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
		self.algorithm = algorithm

	def run(self, observation):
		# wfid = observation.name
		observation.plan = self.plan(observation.name, observation.workflow, self.algorithm)
		yield self.env.timeout(0)

	def plan(self,name, workflow, algorithm):
		wf = Workflow(workflow)
		wfenv = Environment(self.envconfig)
		wf.add_environment(wfenv)
		plan = WorkflowPlan(name, wf, algorithm)
		return plan


class WorkflowPlan(object):
	"""
	WorkflowPlans are used within the Planner, SchedulerA Actors and Cluster Resource. They are higher-level than the
	shadow library representation, as they are a storage component of scheduled tasks, rather than directly representing
	the DAG nature of the workflow. This is why the tasks are stored in queues.
	"""

	def __init__(self, wid, workflow, algorithm):
		self.id = wid
		if algorithm is 'heft':
			self.makespan = shadow_heft(workflow)
		else:
			sys.exit("Other algorithms are not supported")

		# DO Task execution things here
		taskid = 0
		ast = 1
		aft = 2
		self.tasks = []
		task_order = []

		for machine in workflow.machine_alloc:
			machine_tasks = workflow.machine_alloc[machine]
			for task in machine_tasks:
				taskobj = Task(task[taskid])
				taskobj.start = task[ast]
				taskobj.finish = task[aft]
				taskobj.duration = taskobj.start - taskobj.finish
				taskobj.machine_id = machine
				taskobj.exec_order = workflow.execution_order.index(task[taskid])
				taskobj.flops = workflow.graph.nodes[taskobj.id]['flops']
				self.tasks.append(taskobj)
		self.tasks.sort(key=lambda x: x.exec_order)
		self.exec_order = workflow.execution_order
		self.start_time = None
		self.priority = 0

	def __lt__(self, other):
		return self.priority < other.priority

	def __eq__(self, other):
		return self.priority == other.priority

	def __gt__(self, other):
		return self.priority > other.priority

class  TaskStatus(Enum):
	UNSCHEDULED = 1
	SCHEDULED = 2
	FINISHED = 3

class Task(object):
	"""
	Tasks have priorities inheritted from the workflows from which they are arrived; once
	they arrive on the cluster queue, they are workflow agnositc, and are processed according to
	their priority.
	"""

	def __init__(self, tid):
		self.id = tid
		self.start = 0
		self.finish = 0
		self.flops = 0
		self.memory = None
		self.io = 0
		self.machine_id = None
		self.duration = None
		self.exec_order = None
		self.task_state = TaskStatus.UNSCHEDULED

	def __lt__(self, other):
		return self.exec_order < other.exec_order

	def __eq__(self, other):
		return self.exec_order == other.exec_order

	def __gt__(self, other):
		return self.exec_order > other.exec_order
