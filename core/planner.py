import sys
from enum import Enum
from os import path

# sys.path.append(path.abspath('../../shadow'))

from shadow.models.workflow import Workflow as ShadowWorkflow
from shadow.models.environment import Environment as ShadowEnvironment
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

	def plan(self, name, workflow, algorithm):
		workflow = ShadowWorkflow(workflow)
		workflow_env = ShadowEnvironment(self.envconfig)
		workflow.add_environment(workflow_env)
		plan = WorkflowPlan(name, workflow, algorithm, self.env)
		return plan


class WorkflowStatus(int, Enum):
	UNSCHEDULED = 1
	SCHEDULED = 2
	FINISHED = 3


class WorkflowPlan(object):
	"""
	WorkflowPlans are used within the Planner, SchedulerA Actors and Cluster Resource. They are higher-level than the
	shadow library representation, as they are a storage component of scheduled tasks, rather than directly representing
	the DAG nature of the workflow. This is why the tasks are stored in queues.
	"""

	def __init__(self, wid, workflow, algorithm, env):
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

		for task in workflow.tasks:
				taskobj = Task(task.tid, env)
				taskobj.est = task.ast
				taskobj.eft = task.aft
				taskobj.duration = taskobj.eft - taskobj.est
				taskobj.machine_id = task.machine.id
				taskobj.flops = task.flops_demand
				taskobj.pred = list(workflow.graph.predecessors(task))
				self.tasks.append(taskobj)
		self.tasks.sort(key=lambda x: x.ast)
		self.exec_order = workflow.solution.execution_order
		self.start_time = None
		self.priority = 0
		self.status = WorkflowStatus.UNSCHEDULED



	def __lt__(self, other):
		return self.priority < other.priority

	def __eq__(self, other):
		return self.priority == other.priority

	def __gt__(self, other):
		return self.priority > other.priority


class TaskStatus(Enum):
	UNSCHEDULED = 1
	SCHEDULED = 2
	FINISHED = 3


class Task(object):
	"""
	Tasks have priorities inheritted from the workflows from which they are arrived; once
	they arrive on the cluster queue, they are workflow agnositc, and are processed according to
	their priority.
	"""
	# NB I don't want tasks to have null defaults; should we improve on this by initialising
	# everything in a task at once?
	def __init__(self, tid, env):
		"""
		:param tid: ID of the Task object
		:param env: Simulation environment to which the task will be added, and where it will run as a process
		"""
		self.id = tid
		self.env = env
		self.est = 0
		self.eft = 0
		self.ast = -1
		self.aft = -1
		self.machine_id = None
		self.duration = None
		self.exec_order = None
		self.task_status = TaskStatus.UNSCHEDULED
		self.pred = None

		# Machine information that is less important currently (will update this in future versions)
		self.flops = 0
		self.memory = 0
		self.io = 0

	# def __lt__(self, other):
	# 	return self.exec_order < other.exec_order
	#
	# def __eq__(self, other):
	# 	return self.exec_order == other.exec_order
	#
	# def __gt__(self, other):
	# 	return self.exec_order > other.exec_order

	def __hash__(self):
		return hash(self.id)
	# self.cluster.waiting_tasks.remove(self)
	# self.cluster.running_tasks.append(self)
	# self.machine.run(self)

	def do_work(self):
		yield self.env.timeout(self.duration)

		self.finished_timestamp = self.env.now
		print(self.id,'finished at', self.finished_timestamp)
		self.task_status = TaskStatus.FINISHED
		self.machine.stop_task(self)

	def run(self, machine):
		self.started_timestamp = self.env.now
		print(self.id,'started at', self.started_timestamp)
		# THIS IS THE MACHINE TASK
		self.machine = machine
		self.machine.run_task(self)
		self.task_status = TaskStatus.SCHEDULED
		self.process = self.env.process(self.do_work())

# MAchine functions - change these to 'allocate_task_to_machine' and 'remove_task_from_machine'
# def run_task_instance(self, task_instance):
# 	self.cpu -= task_instance.cpu
# 	self.memory -= task_instance.memory
# 	self.disk -= task_instance.disk
# 	self.task_instances.append(task_instance)
# 	self.machine_door = MachineDoor.TASK_IN
#
# def stop_task_instance(self, task_instance):
# 	self.cpu += task_instance.cpu
# 	self.memory += task_instance.memory
# 	self.disk += task_instance.disk
# 	self.machine_door = MachineDoor.TASK_OUT
