# Copyright (C) 23/6/21 RW Bunney

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

LOGGER = logging.getLogger(__name__)


class GreedySchedulingFromPlan(Scheduling):
    """
    Algorithm that Greedily re-allocates a schedule based on run-time
    resource availability.
    """

    def __init__(self):
        super().__init__()
        self.name = "GreedyAlgorithmFromPlan"

    def parse_workflow_plan(self):
        """

        Returns
        -------

        """
        pass

    def __repr__(self):
        return "GreedyAlgorithmFromPlan"

    def run(self, cluster, clock, workflow_plan, existing_schedule,task_pool):
        """
        cluster,
        clock
        workflow_plan
        existing_schedule
        """
        cluster = cluster
        machines = cluster.machines
        workflow_id = workflow_plan.id
        # tasks = cluster.tasks_which_has_waiting_instance
        tasks = workflow_plan.tasks

        # Iterate through immediate predecessors and check that they are
        # finished
        # Schedule as we go
        # Check if there is an overlap between the two sets

        allocations = copy.copy(existing_schedule)
        self.accurate = 0
        self.alternate = 0
        temporary_resources = cluster.current_available_resources()

        for task in tasks:
            # Allocate the first element in the Task list:
            if task.task_status is TaskStatus.UNSCHEDULED:
                # Are we workkflow - delayed?
                if workflow_plan.ast > workflow_plan.est:
                    workflow_plan.status = WorkflowStatus.DELAYED
                if not task.pred:
                    # machine = cluster.dmachine[task.machine]
                    machine = cluster.get_machine_from_id(task.allocated_machine_id)
                    workflow_plan.status = WorkflowStatus.SCHEDULED
                    allocations, temporary_resources = (
                        self._attempt_machine_allocation(
                            cluster, machine, task, allocations,
                            temporary_resources
                        )
                    )
                # The task has predecessors
                else:
                    # If the set of finished tasks does not contain all of the
                    # previous tasks, we cannot start yet.
                    pred = set(task.pred)
                    # finished = set(t.id for t in cluster.tasks['finished'])
                    finished = set(t.id for t in cluster.finished_tasks)
                    # machine = cluster.dmachine[task.machine]
                    machine = cluster.get_machine_from_id(task.allocated_machine_id)
                    # Check if there is an overlap between the two sets
                    if not pred.issubset(finished):
                        # One of the predecessors of 't' is still running
                        continue
                    else:
                        # A machine may not be occupied, but we may have
                        # provisionally allocated it within this scheduling run
                        allocations, temporary_resources = (
                            self._attempt_machine_allocation(
                                cluster, machine, task, allocations,
                                temporary_resources
                            )
                        )

        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            LOGGER.debug("is finished %s", workflow_id)

        return allocations, workflow_plan.status, task_pool

    def to_df(self):
        """
        Create data frame for montior

        Returns
        -------
        df : pd.DataFrame
            Pandas DataFrame object
        """
        df = pd.DataFrame()
        df['alternate'] = [self.alternate]
        df['accurate'] = [self.accurate]
        return df

    def _attempt_machine_allocation(
            self, cluster, machine, task, allocations, temporary_resources
    ):
        """
        If there are no precedence constraints on the current task,
        try allocating to the machine

        Notes
        -----
        If the machine is occupied, we attempt to find an alternative resource
        (this approach is greedy).

        Returns
        -------
        allocations
        temporary_resources

        """
        if cluster.is_occupied(machine):
            if temporary_resources:
                # so greedy we pop the first resource available
                machine = temporary_resources[0]
                allocations[task] = machine
                temporary_resources.remove(machine)
                self.alternate += 1
        else:
            allocations[task] = machine
            temporary_resources.remove(machine)
            self.accurate += 1

        return allocations, temporary_resources
