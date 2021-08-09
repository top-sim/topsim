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

from topsim.core.task import TaskStatus
from topsim.core.planner import WorkflowStatus
from topsim.algorithms.scheduling import Algorithm

logger = logging.getLogger(__name__)


class BatchProcessing(Algorithm):
    """
    Dynamic schedule for a workflowplan that is using a batch-processing
    resource reservation approach without generating a static schedule.

    Attributes
    ----------
    max_resources_split : int
        The number of provisions that can be made on the cluster.
        By default, this is 2 - that is, only two workflows may run on the
        cluster at any given point in time, provided they also do not overlap
        with the number of ingest pipelines, too.

    min_resources_per_workflow: int

    """

    def __init__(self, max_resources_split=2, min_resources_per_workflow=3):
        super().__init__()
        self.max_resources_split = max_resources_split
        self.min_resource_per_workflow = min_resources_per_workflow

    def __repr__(self):
        return "BatchProcessing"

    def run(self, cluster, clock, workflow_plan, existing_schedule):
        """
        Generate a list of allocations for the current timestep using the
        existing schedule as a basis.


        """
        provision = False
        allocations = copy.copy(existing_schedule)
        provision = self._provision_resources(cluster, workflow_plan)
        if clock % 100 == 0 and not provision:
            logger.info(f"{workflow_plan.id} attempted to provision @ {clock}.")
            logger.info(f"{cluster.num_provisioned_obs} existing provs.")
        tasks = workflow_plan.tasks
        if provision:
            temporary_resources = cluster.get_idle_resources(workflow_plan.id)
            for task in tasks:
                if len(temporary_resources) > 0 and task not in allocations:
                    if task.task_status is TaskStatus.UNSCHEDULED:
                        # Pick the next available machine
                        m = temporary_resources[0]
                        if not task.pred:
                            allocations[task] = m
                            temporary_resources.remove(m)
                        else:
                            pred = set(task.pred)
                            finished = set(
                                t.id for t in cluster.get_finished_tasks()
                            )
                            # Check if there isn't an overlap between sets
                            if not pred.issubset(finished):
                                # one of the predecessors is still running
                                continue
                            else:
                                allocations[task] = m
                                temporary_resources.remove(m)

        if len(workflow_plan.tasks) == 0:
            workflow_plan.status = WorkflowStatus.FINISHED
            logger.debug(f'{workflow_plan.id} is finished.')
            cluster.release_batch_resources(workflow_plan.id)
        return allocations, workflow_plan.status

    def to_df(self):
        pass

    def _max_resource_provision(self, cluster):
        """

        Calculate the appropriate number of resources to provision accordingly

        Parameters
        ----------
        n_resources : int
            The total number of resources available to the graph

        Notes
        -----
        The maximum resource provisioning is based on the effective
        percentage of the total number of resources that exist in the
        cluster. As the resources that are used to process the workflows are
        shared with ingest resources, it's important

        Returns
        -------
        prov_resources : int
            Number of resources to provision based on our provisioning heuristic
        """
        available = len(cluster.get_available_resources())
        # Ensure we don't provision more than is acceptable for a single
        # workflow

        #  TODO do We need to make sure there's enough left for ingest to
        #   occur?
        max_allowed = int(len(cluster.dmachine) / self.max_resources_split)
        if available == 0:
            return 0
        if available < max_allowed:
            return available
        else:
            return max_allowed

    def _provision_resources(self, cluster, workflow_plan):
        """
        Given the defined max_resources_split, provision resources

        Note:
        This should only be called once, but it's easier to call it at the
        beginning each time in the case that we have started an allocation
        loop in the scheduler but there are no resources available.


        Returns
        -------

        """
        if cluster.is_observation_provisioned(workflow_plan.id):
#             logger.info(f"{workflow_plan.id} already provisioned.")
            return True
        else:
            if cluster.num_provisioned_obs >= self.max_resources_split:
                return False
            else:
                provision = self._max_resource_provision(cluster)
                if provision < self.min_resource_per_workflow:
                    return False
                else:
                    logger.info(f"{provision} machines for {workflow_plan.id}")
                    return cluster.provision_batch_resources(
                        provision, workflow_plan.id
                    )