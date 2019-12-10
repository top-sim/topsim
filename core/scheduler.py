# Copyright (C) 10/19 RW Bunney

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


class Scheduler(object):
	def __init__(self, env, algorithm, buffer, cluster):
		self.env = env
		# algorithm is a class
		self.algorithm = algorithm
		# self.destroyed = False
		self.workflows = {}
		self.valid_pairs = {}
		self.cluster = cluster
		self.buffer = buffer

	# def attach(self, simulation):
	# 	self.simulation = simulation

	def run(self):
		while True:
			print(self.env.now)
			if self.check_buffer():
				self.process_workflow_in_buffer()
			yield self.env.timeout(1)

	def check_buffer(self):
		if not self.buffer.observations_for_processing.empty():
			print("Workflows currently waiting in the Buffer: {0}".format(self.buffer.observations_for_processing))
			obs = self.buffer.observations_for_processing.get()  # Get oldest observation
			assert obs.plan.start_time >= self.env.now
			self.workflows[obs.plan.id] = obs.plan
			return True
		else:
			return False

	def process_workflow_in_buffer(self):
		# Workflow needs a priority
		if self.workflows:
			print("Scheduler: Currently waiting to process: ", self.workflows)
		else:
			print("Scheduler: Nothing in Buffer to process")

		max = -1
		current_plan = None

		# Note workflows is a workflow-id: workflow-plan key:value pair
		for workflow in self.workflows:
			st = self.workflows[workflow].start_time
			if max == -1 or st < max:
				max = st
				current_plan = self.workflows[workflow]

		if current_plan:
			print("Current plan: ", current_plan.id, current_plan.exec_order)
			while True:
				machine, task = self.algorithm(self.cluster, self.env.now)
				if machine is None or task is None:
					break
				else:
					self.allocate_task(task, machine)


	def unroll_plan(self, plan):
		"""
		The plan object has the exec_order and allocation attributes.
		We need to use these attributes to instantiate the tasks with their respective
		start times and execution order.
		:param plan:
		:return:
		"""
		alloc = plan.allocation


	def allocate_task(self, task, machine):
		"""
		We are going to move away slightly from the 'task initiates its allocation', which was
		the method elected in CloudSimPy; instead, the scheduler performs the allocation (I prefer
		this from a logical/Actor way of thinking').
		:param task:
		:param machine:
		:return:
		"""
		pass


	def _init_workflow(self, workflow):
		# Get the nodes for the workflow from somewhere
		pass

	#
	# def add_workflow(self, workflow):
	# 	print("Adding", workflow, "to workflows")
	# 	self.observations_for_processing.append(workflow)
	# 	print("Waiting workflows", self.observations_for_processing

	@property
	def state(self):
		# Change this to 'workflows scheduled/workflows unscheduled'
		return {
			# 'observations_for_processing': [plan.id for plan in self.workflow_plans]
		}


class Task(object):
	"""
	Tasks have priorities inheritted from the workflows from which they are arrived; once
	they arrive on the cluster queue, they are workflow agnositc, and are processed according to
	their priority.
	"""
	def __init__(self, id):
		self. id = id
		self.start = 0
		self.finish = 0
		self.flops = 0
		self.alloc = None
		self.duration = None
		self.priority = 0  # Default priority

	def __lt__(self, other):
		return self.priority < other.priority

	def __le__(self, other):
		return self.priority <= other.priority

	def __eq__(self, other):
		return self.priority == other.priority

	def __ge__(self, other):
		return self.priority >= other.priority

	def __gt__(self, other):
		return self.priority > other.priority


