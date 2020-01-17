# Copyright (C) 18/10/19 RW Bunney

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

import numpy as np
from core.algorithm import Algorithm
from core.planner import TaskStatus

class FifoAlgorithm(Algorithm):
	def __init__(self, threshold=0.8):
		self.threshold = threshold

	def parse_workflow_plan(self):
		pass

	def __call__(self, cluster, clock, workflow_plan):
		self.cluster = cluster
		machines = self.cluster.machines
		# tasks = cluster.tasks_which_has_waiting_instance
		tasks = workflow_plan.tasks
		candidate_task = None
		candidate_machine = None

		# Iterate through immediate predecessors and check that they are finished
		# Schedule as we go
		for t in self.cluster.running_tasks:
			# Check if the running tasks have finished
			if t.task_status is TaskStatus.FINISHED:
				self.cluster.running_tasks.remove(t)
				self.cluster.finished_tasks.append(t)

		for t in self.cluster.waiting_tasks:
			# Check if a task waiting to be allocated has predecessors still running
			a = t
			pred = set(t.pred)
			running = set(cluster.running_tasks)
		# Check if there is an overlap between the two sets
		for t in tasks:
			# Allocate the first element in the Task list:
			if t.task_status is TaskStatus.UNSCHEDULED and t.est <= clock:
				# Check if task has any predecessors:
				if not t.pred:
					# The first task
					return t.machine_id, t
				# The task has predecessors
				else:
					pred = set(t.pred)
					running = set(cluster.running_tasks)
					# Check if there is an overlap between the two sets
					if pred & running:
						# One of the predecessors of 't' is still running
						return None, None
					else:
						return t.machine_id, t
			# for tp in t.pred:
			# 	if tp.task_status is not TaskStatus.FINISHED:
			# 		continue

		all_candidates = []
		# Get the first task from the list of tasks
		return None, None
		# for machine in machines:
		# 	for task in tasks:
		# 		if machine.accommodate(task):
		# 			all_candidates.append((machine, task))
		# 			if np.random.rand() > self.threshold:
		# 				candidate_machine = machine
		# 				candidate_task = task
		# 				break
		# if len(all_candidates) == 0:
		# 	return None, None
		# if candidate_task is None:
		# 	pair_index = np.random.randint(0, len(all_candidates))
		# 	return all_candidates[pair_index]
		# else:
		# 	return candidate_machine, candidate_task
		#
