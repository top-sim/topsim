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
import logging
import numpy as np
from core.algorithm import Algorithm
from core.planner import TaskStatus,WorkflowStatus

logger = logging.getLogger(__name__)


class Scheduler(object):
	def __init__(self, env, algorithm, buffer, cluster, telescope):
		self.env = env
		self.telescope = telescope
		self.algorithm = algorithm
		self.waiting_observations = []
		self.current_observation = None
		self.cluster = cluster
		self.buffer = buffer
		self.current_plan = None

	# def attach(self, simulation):
	# 	self.simulation = simulation

	def run(self):
		while True:
			# AT THE END OF THE SIMULATION, WE GET STUCK HERE. NEED TO EXIT
			if self.check_buffer() or self.waiting_observations or self.cluster.running_tasks:
				self.schedule_workflows()
			if len(self.waiting_observations) == 0 and not self.telescope.check_observation_status():
				logger.debug("No more waiting workflows")
				break
			yield self.env.timeout(1)

	def check_buffer(self):
		if self.buffer.waiting_observation_list:
			logger.debug(
				"Workflows currently waiting in the Buffer: {0}".format(
					[o.name for o in self.buffer.waiting_observation_list]
				)
			)
			for observation in self.buffer.waiting_observation_list:
				if observation not in self.waiting_observations:
					self.waiting_observations.append(observation)
			return True
		else:
			return False

	def schedule_workflows(self):
		logger.debug('Attempting to schedule workflow to cluster')

		# Min -scheduling time
		minst = -1
		if not self.current_plan:
			for observation in self.waiting_observations:
				st = observation.plan.start_time
				if minst == -1 or st < minst:
					minst = st
					self.current_plan = observation.plan
					self.current_plan.start_time = self.env.now
					self.current_observation = observation
					logger.info("New observation %s scheduled for processing @ Time: %s", observation.plan.id, self.env.now)

		if self.current_plan.status is WorkflowStatus.FINISHED:
			self.waiting_observations.remove(self.current_observation)
			self.buffer.request_observation_data_from_buffer(self.current_observation)
			self.buffer.waiting_observation_list.remove(self.current_observation)
			logger.info('%s finished processing @ %s', self.current_observation.name, self.env.now)
			self.current_plan = None
			self.current_observation = None

		if self.waiting_observations:
			for observation in self.waiting_observations:
				if observation.plan.status is WorkflowStatus.FINISHED:
					self.waiting_observations.remove(self.current_observation)
					self.buffer.waiting_observation_list.remove(self.current_observation)
			logger.debug("Currently waiting to process: %s", self.waiting_observations)
		else:
			logger.debug("Nothing in Buffer to process")

		if self.current_plan:
			logger.debug("Current plan: %s, %s ", self.current_plan.id, self.current_plan.exec_order)

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
			'observations_for_processing': [observation.plan.id for observation in self.waiting_observations]
		}
