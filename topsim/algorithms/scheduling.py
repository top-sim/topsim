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

import logging

from topsim.core.algorithm import Algorithm
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

logger = logging.getLogger(__name__)


class FifoAlgorithm(Algorithm):
	def __init__(self, threshold=0.8):
		self.threshold = threshold

	def parse_workflow_plan(self):
		pass

	def __call__(self, cluster, clock, workflow_plan):
		"""

		:param cluster:
		:param clock:
		:param workflow_plan: a (Workflow-id, workflow-plan) tuple.
		:return:
		"""
		self.cluster = cluster
		machines = self.cluster.machines
		workflow_id = workflow_plan.id
		# tasks = cluster.tasks_which_has_waiting_instance
		tasks = workflow_plan.tasks

		# Iterate through immediate predecessors and check that they are finished
		# Schedule as we go
		for t in self.cluster.running_tasks:
			# Check if the running tasks have finished
			# TODO check if expected run time is the same as the 'assigned'
			#  runtime (i.e. we have a 'delay'); if not, we have a delay and
			# we need to return 'current workflow execution status'
			if t.task_status is TaskStatus.FINISHED:
				self.cluster.stop_task(t)
				self.cluster.running_tasks.remove(t)
				self.cluster.finished_tasks.append(t)
				workflow_plan.tasks.remove(t)
		if len(workflow_plan.tasks) == 0:
			workflow_plan.status = WorkflowStatus.FINISHED
			logger.debug(workflow_id, "is finished")

		# Check if there is an overlap between the two sets
		for t in tasks:
			# Allocate the first element in the Task list:
			if t.task_status is TaskStatus.UNSCHEDULED and t.est + workflow_plan.start_time <= clock:
				# Check if task has any predecessors:
				# TODO check to make sure that the Machine is unoccupied; if
				#  it is occupied, then we are DELAYED and need to
				#  communicate this
				if not t.pred:
					# The first task
					return t.machine_id, t
				# The task has predecessors
				else:
					pred = set(t.pred)
					running = set([t.id for t in cluster.running_tasks])
					# Check if there is an overlap between the two sets
					if pred & running:
						# One of the predecessors of 't' is still running
						return None, None
					else:
						return t.machine_id, t, workflow_plan.status

		return None, None

def check_workflow_progress(cluster, workflow_plan):
	for t in cluster.running_tasks:
		if t.current_finish_time > t.estimated_finish_time:
			return WorkflowStatus.DELAYED

class GlobalDagDelayHeuristic(Algorithm):

	"""
	Implementation of the bespoke Delay heuristic I have been working on
	"""