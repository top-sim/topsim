# Copyright (C) 12/7/21 RW Bunney

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

import copy
import logging
import networkx as nx

from topsim.core.task import TaskStatus
from topsim.core.planner import WorkflowStatus
from topsim.algorithms.scheduling import Scheduling

logger = logging.getLogger(__name__)


class QueueProcessing(Scheduling):
    """
    Dynamic schedule for a workflowplan that is using a queue-model without
    the use of a static schedule.

    Attributes
    ----------
    max_resource_partitions : int
        The number of provisions that can be made on the cluster.
        By default, this is 2 - that is, only two workflows may run on the
        cluster at any given point in time, provided they also do not overlap
        with the number of ingest pipelines, too.

    min_resources_per_workflow: int
        The

    resource_split: dict

    """

    def __init__(
        self,
        max_resource_partitions=1,
        min_resources_per_workflow=3,
        resource_split=None,
    ):
        super().__init__()
        self.max_resources_split = max_resource_partitions
        self.min_resource_per_workflow = min_resources_per_workflow
        self.resource_split = resource_split

    def __repr__(self):
        return "QueueProcessing"

    def run(self, cluster, clock, workflow_plan, existing_schedule, task_pool):
        """
        Generate a list of allocations for the current timestep using the
        existing schedule as a basis.
        """

        allocations = copy.copy(existing_schedule)

        if not task_pool:
            for task in workflow_plan.tasks:
                if not list(workflow_plan.graph.predecessors(task)):
                    task_pool.add(task)
        removed = set()
        added = set()
        temporary_resources = cluster.get_available_resources()
        # The starting number of temporary resources is the maximum
        # number of (greedy) allocations we can make
        max_allocations_iteration = len(temporary_resources)
        for task in task_pool:
            # If we have exhausted all possible allocations for this
            # timest ep, there no need to keep iterating
            if len(allocations) >= max_allocations_iteration:
                break
            if len(temporary_resources) > 0 and task not in allocations:
                if task.task_status is TaskStatus.UNSCHEDULED:
                    # Pick the next available machine
                    m = temporary_resources[0]
                    # If there are no predecessors, we can schedule
                    # without issue
                    if not list(workflow_plan.graph.predecessors(task)):
                        # if not task.pred:
                        tduration = int(task.flops / m.cpu)
                        allocations[task] = m
                        temporary_resources.remove(m)
                        removed.add(task)
                        added.update(workflow_plan.graph.successors(task))
                    else:
                        pred = list(workflow_plan.graph.predecessors(task))
                        count = 0
                        for p in pred:
                            if cluster.is_task_finished(p):
                                count += 1
                        if count < len(list(pred)):
                            continue
                        else:
                            allocations[task] = m
                            temporary_resources.remove(m)
                            removed.add(task)
                            added.update(workflow_plan.graph.successors(task))
        task_pool -= removed
        task_pool.update(added)
        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            logger.debug(f"{workflow_plan.id} is finished.")
            cluster.release_batch_resources(workflow_plan.id)
        return allocations, workflow_plan.status, task_pool

    def to_df(self):
        pass
