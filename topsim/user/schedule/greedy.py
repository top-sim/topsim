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

from topsim.core.algorithm import Algorithm
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

LOGGER = logging.getLogger(__name__)


class GreedyAlgorithmFromPlan(Algorithm):
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

    def run(self, cluster, clock, workflow_plan, existing_schedule):
        """
        cluster,
        clock
        workflow_plan
        existing_schedule
        """
        self.cluster = cluster
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
        temporary_resources = self.cluster.current_available_resources()

        for task in tasks:
            # Allocate the first element in the Task list:
            if task.task_status is TaskStatus.UNSCHEDULED:
                # Are we workkflow - delayed?
                if workflow_plan.ast > workflow_plan.est:
                    workflow_plan.status = WorkflowStatus.DELAYED
                if not task.pred:
                    machine = cluster.dmachine[task.machine]
                    workflow_plan.status = WorkflowStatus.SCHEDULED
                    if self.cluster.is_occupied(machine):
                        # Is there another machine
                        if temporary_resources:
                            machine = temporary_resources[0]
                            allocations[task] = machine
                            temporary_resources.remove(machine)
                            self.alternate += 1
                    else:
                        allocations[task] = machine
                        temporary_resources.remove(machine)
                        self.accurate += 1

                # The task has predecessors
                else:
                    # If the set of finished tasks does not contain all of the
                    # previous tasks, we cannot start yet.
                    pred = set(task.pred)
                    finished = set(t.id for t in cluster.tasks['finished'])

                    machine = cluster.dmachine[task.machine]

                    # Check if there is an overlap between the two sets
                    if not pred.issubset(finished):
                        # One of the predecessors of 't' is still running
                        continue
                    else:
                        # A machine may not be occupied, but we may have
                        # provisionally allocated it within this scheduling run
                        if self.cluster.is_occupied(machine):
                            if temporary_resources:
                                machine = temporary_resources[0]
                                allocations[task] = machine
                                temporary_resources.remove(machine)
                                self.alternate += 1
                        else:
                            allocations[task] = machine
                            temporary_resources.remove(machine)
                            self.accurate += 1

        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            LOGGER.debug("is finished %s", workflow_id)

        return allocations, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
        df['alternate'] = [self.alternate]
        df['accurate'] = [self.accurate]
        return df
