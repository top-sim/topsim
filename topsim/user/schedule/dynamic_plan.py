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
import json
import pandas as pd
import networkx as nx

from topsim.algorithms.scheduling import Scheduling
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

logger = logging.getLogger(__name__)


class DynamicSchedulingFromPlan(Scheduling):
    """
    This plan
    """

    def __init__(self,
                 max_resource_partitions=1,
                 min_resources_per_workflow=2,
                 resource_split=None,
                 ignore_ingest=False,
                 use_workflow_dop=False
                 ):
        super().__init__()
        self.accurate = 0
        self.alternate = 0
        self._report = True
        self.max_resources_split = max_resource_partitions
        self.min_resource_per_workflow = min_resources_per_workflow
        self.ignore_ingest = ignore_ingest
        self.resource_split = resource_split
        self.use_workflow_dop = use_workflow_dop

    def __str__(self):
        return "DynamicAllocationFromFixedStaticPlan"

    def to_string(self):
        return self.__str__()

    def run(self, cluster, planner, clock, workflow_plan, existing_schedule, task_pool,
            **kwargs):
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
        observation = kwargs['observation']

        provision = self._provision_resources(cluster, observation)
        # if clock % 100 == 0 and not provision:
            # logger.info(f"{observation.name} attempted to provision @ {clock}.")


        allocations = copy.copy(existing_schedule)
        if not provision:
           return allocations, workflow_plan, task_pool #allocations, workflow_plan.status, task_pool
        if not workflow_plan and provision:
            # for m in cluster.get_idle_resources(observation.name):
            #     logger.info("Provisioning: %s", m)
            workflow_plan = planner.run(observation, None, None)

        replace = False
        if not task_pool:
            for task in workflow_plan.tasks:
                if not list(workflow_plan.graph.predecessors(task)):
                    task_pool.add(task)
        removed = set()
        added = set()
        if provision:
            self.accurate = 0
            self.alternate = 0
            temporary_resources = cluster.get_idle_resources(observation.name)
            if self._report:
                logger.info("%s available resources", len(temporary_resources))
                self._report = False
            max_allocations_iteration = len(temporary_resources)
            for task in sorted(task_pool, key=lambda x: x.est):
                # If we have exhausted all possible allocations for this
                # timestep, there no need to iterat
                if len(allocations) >= max_allocations_iteration:
                    break
                if (
                        task.task_status is TaskStatus.UNSCHEDULED
                        and task not in allocations
                        and len(temporary_resources) > 0
                ):
                    # Are we workflow - delayed?
                    # if workflow_plan.ast > workflow_plan.est:
                    #     workflow_plan.status = WorkflowStatus.DELAYED
                    # Task has no predecssors
                    machine = cluster.get_machine_from_id(task.allocated_machine_id)
                    if machine not in temporary_resources:
                        continue
                    if not list(workflow_plan.graph.predecessors(task)):
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
                        pred = list(
                            workflow_plan.graph.predecessors(task)
                        )  # set(task.pred)
                        count = 0
                        for p in pred:
                            if cluster.is_task_finished(p):
                                count += 1
                        if count < len(pred):
                            # One of the predecessors of 't' is still running
                            continue
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
            logger.debug("is finished %s", workflow_plan.id)
            cluster.release_batch_resources(workflow_plan.id)
        return allocations, workflow_plan, task_pool

    def to_df(self):
        df = pd.DataFrame()
        df["alternate"] = [self.alternate]
        df["accurate"] = [self.accurate]
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

    def _max_resource_provision(self, cluster, observation=None):
        """

        Calculate the appropriate number of resources to provision accordingly

        Two approaches here based on differing requirements:

        Resources Split

        Max Resources Split

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
        # TODO consider making the cluster initialised with the max ingest resources for the simulation
        # This can be used to ensure we never dig into ingest resources?
        available = len(cluster.get_available_resources())
        # logger.info("Available resources are: %s", available)
        # Ensure we don't provision more than is acceptable for a single
        # workflow

        #  TODO do We need to make sure there's enough left for ingest to
        #   occur?
        if self.resource_split:
            min_resource_limit, max_resource_limit = self.resource_split[observation.workflow_plan.id]
            if min_resource_limit > len(cluster):
                raise RuntimeError("Minimum resource demand is not supported by cluster")
            elif available == 0:
                return 0
            elif available < min_resource_limit:
                return 0
            else:
                return min(available, max_resource_limit)

        else:
            if self.ignore_ingest:
                self.ingest_requirements = 0

            if len(cluster) == self.LOW_MAX_RESOURCES:
                self.ingest_requirements = self.LOW_REALTIME_RESOURCES
            
            if self.use_workflow_dop:
                with open(observation.workflow, 'r') as infile:
                    wfconfig = json.load(infile)
                graph = nx.readwrite.json_graph.node_link_graph(wfconfig['graph'])

                graph_dop = (max(graph.out_degree(list(graph.nodes)),
                             key=lambda x: x[1]))[1] / 2

                min_resources = int(graph_dop)
                if min_resources == len(cluster):
                    min_resources = int(graph_dop/2)
                if available >= min_resources:
                    return min_resources

            else:
                max_allowed = int(
                    len(cluster) / self.max_resources_split) - self.ingest_requirements

                if max_allowed == 0 and self.ingest_requirements == len(cluster):
                    # We will never be allowed to ingest anything unless we allow resources!
                    max_allowed = len(cluster)
                if available == 0:
                    return 0
                if available < max_allowed:
                    return available
                else:
                    return max_allowed

    def _provision_resources(self, cluster, observation):
        """
        Given the defined max_resources_split, provision resources

        Note:
        This should only be called once, but it's easier to call it at the
        beginning each time in the case that we have started an allocation
        loop in the scheduler but there are no resources available.


        Returns
        -------

        """
        if cluster.is_observation_provisioned(observation.name):
            # logger.info(f"{workflow_plan.id} already provisioned.")
            return True
        else:
            if cluster.num_provisioned_obs < self.max_resources_split:
                provision = self._max_resource_provision(cluster, observation)
                if provision < self.min_resource_per_workflow:
                    return False
                else:
                    logger.info(f"{provision} machines provisioned for {observation.name}")
                    return cluster.provision_batch_resources(provision,
                                                             observation.name)
            else:
                return False


"""

        graph_dop = (max(workflow.graph.out_degree(list(workflow.graph.nodes)), key=lambda x: x[1]))[1]/2
        logging.info("Graph parallelism is %d, updating resources from %d", graph_dop,
                 len(available_resources))

        available_resources = available_resources[:int(graph_dop)]
"""


class GlobalDagDelayHeuristic(Scheduling):
    """
    Implementation of the bespoke Delay heuristic I have been working on
    """

    def run(self, cluster, clock, workflow_plan):
        return None, None, workflow_plan.status

    def to_df(self):
        df = pd.DataFrame()
        return df
