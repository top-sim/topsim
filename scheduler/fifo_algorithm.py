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

class FifoAlgorithm(Algorithm):
	def __init__(self, threshold=0.8):
		self.threshold = threshold

	def parse_workflow_plan(self):
		pass

	def __call__(self, cluster, clock, workflow_plan):
		machines = cluster.machines
		# tasks = cluster.tasks_which_has_waiting_instance
		tasks = workflow_plan.tasks
		candidate_task = None
		candidate_machine = None

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
