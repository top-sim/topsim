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
from queue import PriorityQueue


class Scheduler(object):
	def __init__(self, env, algorithm, buffer, cluster):
		self.env = env
		# algorithm is a class
		self.algorithm = algorithm
		# self.destroyed = False
		self.workflows = {}
		# self.valid_pairs = {}
		self.cluster = cluster
		self.buffer = buffer

	# def attach(self, simulation):
	# 	self.simulation = simulation

	def run(self):
		while True:
			print('Current time:', self.env.now)
			if self.check_buffer() or self.workflows:
				self.process_workflows()
			yield self.env.timeout(1)

	def check_buffer(self):
		if not self.buffer.observations_for_processing.empty():
			print("Workflows currently waiting in the Buffer: {0}".format(self.buffer.observations_for_processing))
			obsplan = self.buffer.observations_for_processing.get()  # Get oldest observation
			assert obsplan.start_time >= self.env.now
			self.workflows[obsplan.id] = obsplan
			return True
		else:
			return False

	def process_workflows(self):
		# Workflow needs a priority
		if self.workflows:
			print("Scheduler: Currently waiting to process: ", self.workflows)
		else:
			print("Scheduler: Nothing in Buffer to process")

		minst = -1
		current_plan = None

		# Note workflows is a workflow-id: workflow-plan key:value pair
		for workflow in self.workflows:
			st = self.workflows[workflow].start_time
			if minst == -1 or st < minst:
				minst = st
				current_plan = self.workflows[workflow]
		# if self.env.now == 65:
		# 		x = 1
		if current_plan:
			print("Current plan: ", current_plan.id, current_plan.exec_order)

			while True:
				machine, task = self.algorithm(self.cluster, self.env.now, current_plan)
				if machine is None or task is None:
					break
				else:
					self.allocate_task(task, machine)

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

	#
	# def add_workflow(self, workflow):
	# 	print("Adding", workflow, "to workflows")
	# 	self.observations_for_processing.append(workflow)
	# 	print("Waiting workflows", self.observations_for_processing

	def print_state(self):
		# Change this to 'workflows scheduled/workflows unscheduled'
		return {
			'observations_for_processing': [plan.id for plan in self.workflows]
		}





