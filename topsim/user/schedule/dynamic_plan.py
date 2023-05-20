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
import copy
import logging
import pandas as pd

from topsim.algorithms.scheduling import Scheduling
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

logger = logging.getLogger(__name__)


class DynamicSchedulingFromPlan(Scheduling):
    """
    This plan
    """
    def __init__(self):
        super().__init__()
        self.accurate = 0
        self.alternate = 0

    def __repr__(self):
        return "DynamicAlgorithmFromPlan"

    def run(self, cluster, clock, workflow_plan, existing_schedule,task_pool):
        """
        Iterate through immediate predecessors and check that they are finished
        Schedule as we go check if there is an overlap between the two sets

        Parameters
        ----------
        existing_schedule
        cluster : :py:obj:`topsim.core.Cluster object`
            The current cluster
        clock : int
            The current time in the simulation
        workflow_plan : topsim.planner.WorkflowPlan object
            The workflow plan devised

        Returns
        -------
        allocations, WorkflowStatus, false
        """
        cluster = cluster
        machines = cluster.machines
        workflow_id = workflow_plan.id
        tasks = workflow_plan.tasks
        replace = False
        allocations = copy.copy(existing_schedule)
        # allocations = copy.copy(existing_schedule)
        if not task_pool:
            for task in workflow_plan.tasks:
                if not list(workflow_plan.graph.predecessors(task)):
                    task_pool.add(task)
        removed = set()
        added = set()
        self.accurate = 0
        self.alternate = 0
        temporary_resources = cluster.get_available_resources()
        max_allocations_iteration = len(temporary_resources)
        for task in sorted(task_pool, key=lambda x:x.est):
            if len(allocations) >= max_allocations_iteration:
                break
            if (task.task_status is TaskStatus.UNSCHEDULED and
                    task not in allocations and len(temporary_resources) > 0):
                # Are we workflow - delayed?
                if workflow_plan.ast > workflow_plan.est:
                    workflow_plan.status = WorkflowStatus.DELAYED
                # Task has no predecssors
                machine = cluster.get_machine_from_id(task.allocated_machine_id)
                if machine not in temporary_resources:
                    continue
                if not list(workflow_plan.graph.predecessors(task)): # if not task.pred:
                    # TODO Reconsider using this method in the scheduling
                    # We should just use it in the Scheduler
                    workflow_plan.status = WorkflowStatus.SCHEDULED
                    # We do not update the allocations
                    allocations[task] = machine
                    self.accurate += 1
                    temporary_resources.remove(machine)
                    removed.add(task)
                    added.update(workflow_plan.graph.successors(task))
                # The task has predecessors
                else:
                    # If the set of finished tasks does not contain all
                    # of the previous tasks, we cannot start yet.
                    pred = list(workflow_plan.graph.predecessors(task)) # set(task.pred)
                    count = 0
                    for p in pred:
                        if cluster.is_task_finished(p):
                            count += 1
                    if count < len(pred):
                        # One of the predecessors of 't' is still running
                        continue
                    # machine = cluster.get_machine_from_id(
                    #     task.allocated_machine_id
                    # )
                    # finished = set(t.id for t in cluster.finished_tasks)
                    # # Check if there is an overlap between the two sets
                    # if not pred.issubset(finished):
                    #     # One of the predecessors of 't' is still running
                    #     continue
                    else:
                        allocations[task] = machine
                        temporary_resources.remove(machine)
                        removed.add(task)
                        added.update((workflow_plan.graph.successors(task)))
                        self.accurate += 1

        task_pool -= removed
        task_pool.update(added)
        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            logger.debug("is finished %s", workflow_id)
        return allocations, workflow_plan.status, task_pool

    def to_df(self):
        df = pd.DataFrame()
        df['alternate'] = [self.alternate]
        df['accurate'] = [self.accurate]
        return df

    def is_machine_occupied(self, machine):
        """
        Check the custer to determine if the machinen we have just selected is
        available.
        Returns
        -------
        True if machine is occupied
        """
        return self.cluster.is_occupied(machine)


class GlobalDagDelayHeuristic(Scheduling):
    """
    Implementation of the bespoke Delay heuristic I have been working on
    """

    def run(self, cluster, clock, workflow_plan):
        return None, None, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
        return df
