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


# TODO Update Algorithm class inheritance structure
# TODO Algorithm returns allocation based on algorithm and current state.
# TODO this is a Tuple
# TODO 'algorithm_object_instance.process_allocation(current_plan)'

# TODO Algorithms should 'process_plan' and then also have an 'if things
# aren't going according to plan, go to dynamic scheduling.

# This could be something we do in the 'scheduler' - process_plan_allocation,
# which does allocation when we are on time. Otherwise, we go to the scheduler?
# I think we keep it simple at the moment and maintain the current approach.

# TODO TESTS - IF MACHINE SELECTED IS OCCUPIED, RETURN NONE
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
                    t.est + workflow_plan.start_time <= clock:
                # Check if task has any predecessors:
                # TODO check to make sure that the Machine is unoccupied; if
                #  it is occupied, then we are DELAYED and need to
                #  communicate this
                if not t.pred:
                    machine = cluster.dmachine[t.machine_id.id]
                    workflow_plan.status = WorkflowStatus.SCHEDULED
                    if self.is_machine_occupied(machine):
                        return None, None, workflow_plan.status
                    return machine, t, workflow_plan.status
                # The task has predecessors
                else:
                    pred = set(t.pred)
                    # running = set([t.id for t in cluster.tasks['running']])
                    #If the set of finished tasks does not contain all of the
                    # previous tasks, we cannot start yet.
                    finished = set(t.id for t in cluster.tasks['finished'])
                    # Check if there is an overlap between the two sets
                    if not pred.issubset(finished):
                        # One of the predecessors of 't' is still running
                        return None, None, workflow_plan.status
                    else:
                        machine = cluster.dmachine[t.machine_id.id]
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


def check_workflow_progress(cluster, workflow_plan):
    for t in cluster.tasks['running']:
        if t.current_finish_time > t.estimated_finish_time:
            return WorkflowStatus.DELAYED


class GlobalDagDelayHeuristic(Algorithm):
    """
    Implementation of the bespoke Delay heuristic I have been working on
    """

    def __call__(self, cluster, clock, workflow_plan):
        return None, None, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
        return df
