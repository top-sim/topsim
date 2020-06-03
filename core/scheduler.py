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

import numpy as np
from core.algorithm import Algorithm
from core.planner import TaskStatus,WorkflowStatus


class Scheduler(object):
	def __init__(self, env, algorithm, buffer, cluster, telescope):
		self.env = env
		self.telescope = telescope
		# algorithm is a class
		self.algorithm = algorithm
		# self.destroyed = False
		self.workflow_plans = []
		# self.valid_pairs = {}
		self.cluster = cluster
		self.buffer = buffer
		self.current_plan = None

	# def attach(self, simulation):
	# 	self.simulation = simulation

	def run(self):
		while True:
			# print('Current time:', self.env.now)
			# AT THE END OF THE SIMULATION, WE GET STUCK HERE. NEED TO EXIT
			if self.check_buffer() or self.workflow_plans or self.cluster.running_tasks:
				self.schedule_workflows()
			yield self.env.timeout(1)
			if len(self.workflow_plans) == 0 and not self.telescope.check_observation_status():
				print("No more workflows")
				break

	def check_buffer(self):
		if not self.buffer.observations_for_processing.empty():
			print("Workflows currently waiting in the Buffer: {0}".format(self.buffer.observations_for_processing))
			obsplan = self.buffer.observations_for_processing.get()  # Get oldest observation
			assert obsplan.start_time >= self.env.now
			self.workflow_plans.append(obsplan)
			return True
		else:
			return False

	def schedule_workflows(self):
		# Workflow needs a priority
		# for workflow in self.workflow_plans:
		# 	cplan = workflow
		# 	if cplan.status is WorkflowStatus.FINISHED:

		# Min -scheduling time
		minst = -1
		if not self.current_plan:
			for workflow_plan in self.workflow_plans:
				st = workflow_plan.start_time
				if minst == -1 or st < minst:
					minst = st
					self.current_plan = workflow_plan
					self.current_plan.start_time = self.env.now
					print("New observation {0} scheduled for processing".format(self.current_plan.id), self.env.now)

		if self.current_plan.status is WorkflowStatus.FINISHED:
			self.workflow_plans.remove(self.current_plan)
			self.current_plan = None

		if self.workflow_plans:
			for workflow_plan in self.workflow_plans:
				if workflow_plan.status is WorkflowStatus.FINISHED:
					self.workflow_plans.remove(self.current_plan)
			print("Scheduler: Currently waiting to process: ", self.workflow_plans)
		else:
			print("Scheduler: Nothing in Buffer to process")

		# Note workflows is a {workflow-id: workflow-plan} key:value pair

		# This is the current 'priority' - we determine which workflow has been waiting the longest


		if self.current_plan:
			print("Current plan: ", self.current_plan.id, self.current_plan.exec_order)

			while True:
				machine, task = self.algorithm(self.cluster, self.env.now, self.current_plan)
				if machine is None or task is None:
					break
				else:
					# Runs the task on the machine
					task.run(self.find_appropriate_machine_in_cluster(machine))
					if task.task_status is TaskStatus.SCHEDULED:
						self.cluster.running_tasks.append(task)

	# When we run tasks we want to run it on a given machine on the cluster, which the task does not
	# have access to unless we pass it to the class (which seems a bit ridiculous)

	# get task to run

	def find_appropriate_machine_in_cluster(self, machine_id):
		for machine in self.cluster.machines:
			if machine.id == machine_id:
				return machine



	#
	# def add_workflow(self, workflow):
	# 	print("Adding", workflow, "to workflows")
	# 	self.observations_for_processing.append(workflow)
	# 	print("Waiting workflows", self.observations_for_processing

	def print_state(self):
		# Change this to 'workflows scheduled/workflows unscheduled'
		return {
			'observations_for_processing': [plan.id for plan in self.workflow_plans]
		}
