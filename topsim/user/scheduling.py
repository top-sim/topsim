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
import pandas as pd

from topsim.core.algorithm import Algorithm
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

logger = logging.getLogger(__name__)


class GreedyAlgorithmFromPlan(Algorithm):
    def __init__(self, threshold=0.8):
        self.threshold = threshold

    def parse_workflow_plan(self):
        pass

    def __repr__(self):
        return "GreedyAlgorithmFromPlan"

    def __call__(self, cluster, clock, workflow_plan):
        """
        :param cluster:
        :param clock:
        :param workflow_plan: a (Workflow-id, workflow-plan) tuple.
        :return:
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

        curr_allocs = []

        for t in tasks:
            # Allocate the first element in the Task list:
            if t.task_status is TaskStatus.UNSCHEDULED and \
                    t.est + workflow_plan.ast <= clock:
                # Are we workkflow - delayed?
                if workflow_plan.ast > workflow_plan.est:
                    workflow_plan.status = WorkflowStatus.DELAYED
                if not t.pred:
                    machine = cluster.dmachine[t.machine.id]
                    workflow_plan.status = WorkflowStatus.SCHEDULED
                    if self.is_machine_occupied(machine):
                        # Is there another machine
                        if self.cluster.resources['available']:
                            machine = self.cluster.resources['available'][0]
                            return machine, t, workflow_plan.status
                        return None, None, workflow_plan.status
                    return machine, t, workflow_plan.status
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
                        if self.cluster.is_occupied(machine):
                            return None, None, workflow_plan.status
                        return machine, t, workflow_plan.status

        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            logger.debug("is finished %s", workflow_id)

        return None, None, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
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

    def __call__(self, cluster, clock, workflow_plan):
        return None, None, WorkflowStatus.SCHEDULED

    def to_df(self):
        df = pd.DataFrame()
        return df


class GlobalDagDelayHeuristic(Algorithm):
    """
    Implementation of the bespoke Delay heuristic I have been working on
    """

    def __call__(self, cluster, clock, workflow_plan):
        return None, None, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
        return df
