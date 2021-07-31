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

from topsim.core.algorithm import Algorithm
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

logger = logging.getLogger(__name__)


class DynamicAlgorithmFromPlan(Algorithm):
    """
    This plan
    """
    def __init__(self):
        super().__init__()
        self.accurate = 0
        self.alternate = 0

    def __repr__(self):
        return "DynamicAlgorithmFromPlan"

    def run(self, cluster, clock, workflow_plan, existing_schedule):
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
        self.accurate = 0
        self.alternate = 0
        for task in tasks:
            if task not in allocations:
                if (task.task_status is TaskStatus.UNSCHEDULED and
                        task not in allocations):
                    # Are we workkflow - delayed?
                    if workflow_plan.ast > workflow_plan.est:
                        workflow_plan.status = WorkflowStatus.DELAYED
                    if not task.pred:
                        machine = cluster.dmachine[task.machine]
                        workflow_plan.status = WorkflowStatus.SCHEDULED
                        # We do not update the allocations
                        allocations[task] = machine
                        self.accurate += 1

                    # The task has predecessors
                    else:
                        # If the set of finished tasks does not contain all
                        # of the previous tasks, we cannot start yet.
                        pred = set(task.pred)
                        machine = cluster.dmachine[task.machine]
                        finished = set(t.id for t in cluster.tasks['finished'])

                        # Check if there is an overlap between the two sets
                        if not pred.issubset(finished):
                            # One of the predecessors of 't' is still running
                            continue
                        else:
                            allocations[task] = machine
                            self.accurate += 1

        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            logger.debug("is finished %s", workflow_id)

        return allocations, workflow_plan.status

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


class BatchProcessing(Algorithm):
    """
    Mimic a batch-processing heuristic, in which tasks are allocated to an
    available resource if it matches the requirements. There is no checking
    of task times or ESTs/EFTs; we just ensure that the tasks' precedence
    constraints are met.
    """

    def run(self, cluster, clock, workflow_plan, existing_schedule):
        if workflow_plan.algorithm != 'batch':
            raise RuntimeError("Workflow Plan is not compatible with this "
                               "dynamic scheduler")
        else:
            allocated_machines = cluster.resources_for_batch_processing()

        for t in workflow_plan.tasks:
            # Allocate the first element in the Task list:
            if t.task_status is TaskStatus.UNSCHEDULED:
                # Are we workkflow - delayed?
                if workflow_plan.ast > workflow_plan.est:
                    workflow_plan.status = WorkflowStatus.DELAYED
                if not t.pred:
                    machine = cluster.dmachine[t.machine.id]

                # The task has predecessors
                else:
                    # If the set of finished tasks does not contain all of the
                    # previous tasks, we cannot start yet.

                    pred = set(t.pred)
                    finished = set(t.id for t in cluster.tasks['finished'])

                    # Check if there is an overlap between the two sets
                    if not pred.issubset(finished):
                        # One of the predecessors of 't' is still running
                        continue
                    else:
                        machine = cluster.dmachine[t.machine.id]
                        if cluster.is_occupied(machine):
                            return None, None, workflow_plan.status
                        return machine, t, workflow_plan.status

        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            logger.debug("is finished %s", workflow_plan)

        return None, None, WorkflowStatus.SCHEDULED

    def to_df(self):
        df = pd.DataFrame()
        return df


class GlobalDagDelayHeuristic(Algorithm):
    """
    Implementation of the bespoke Delay heuristic I have been working on
    """

    def run(self, cluster, clock, workflow_plan):
        return None, None, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
        return df
