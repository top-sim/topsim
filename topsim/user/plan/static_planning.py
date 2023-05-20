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

import logging
import copy
import networkx as nx

from topsim.algorithms.planning import Planning
from topsim.core.planner import WorkflowPlan, WorkflowStatus
from topsim.core.task import Task

from shadow.algorithms.heuristic import heft, fcfs, pheft
from shadow.models.workflow import Workflow, Environment

LOGGER = logging.getLogger(__name__)


class SHADOWPlanning(Planning):
    """
    Parameters
    ----------
    observation
    algorithm
    buffer
    delay_model
    """

    def __init__(self, algorithm, delay_model=None):

        super().__init__(algorithm, delay_model)
        # self.observation = observation
        # self.algorithm = algorithm
        # self.buffer = buffer
        # self.delay_model = delay_model

    def generate_plan(self, clock, cluster, buffer, observation, max_ingest):
        """
        For this StaticPlanning example, we are using the SHADOW static
        scheduling library to produce static plans.

        Parameters
        ----------
        observation
        buffer
        cluster
        clock : int
            Current simulation time (usually provided through `env.now`)
        max_ingest
        Returns
        -------

        """
        if observation.ast is None:
            raise RuntimeError(
                f'Observation AST must be updated before plan'
            )
        workflow = self._initialise_shadow_workflows(observation,cluster)
        solution = self._run_scheduling(workflow)

        est = self._calc_workflow_est(observation, buffer)
        eft = solution.makespan
        mapping = {}
        tasks = []
        exec_order_mapping = {}

        for task in solution.task_allocations:
            allocation = solution.task_allocations.get(task)
            tid = self._create_observation_task_id(task.tid, observation, clock)
            exec_order_mapping[tid] = task.tid
            dm = copy.copy(self.delay_model)
            pred = list(workflow.graph.predecessors(task))
            predecessors = [
                self._create_observation_task_id(
                    x.tid, observation, clock
                ) for x in pred
            ]
            edge_costs = {}
            # Get the data transfer costs
            data = dict(workflow.graph.pred[task])
            for element in data:
                nm = self._create_observation_task_id(
                    element.tid, observation, clock
                )
                val = data[element]["transfer_data"]
                edge_costs[nm] = val

            taskobj = Task(
                tid,
                allocation.ast,
                allocation.aft,
                allocation.machine.id,
                predecessors,
                task.flops_demand, task.io_demand, edge_costs,
                dm
            )
            mapping[task] = taskobj
            tasks.append(taskobj)
        new_graph = nx.relabel_nodes(workflow.graph, mapping)
        tasks.sort(key=lambda x: x.est)
        exec_order = [
            self._create_observation_task_id(x, observation, clock)
            for x in solution.execution_order
        ]

        return WorkflowPlan(
            observation.name, est, eft, tasks, exec_order,
            WorkflowStatus.SCHEDULED, max_ingest, new_graph
        )

    def to_df(self):
        """
        Produce output to be amalgamated into the global simulation data
        frame produced by the Monitor
        """
        pass

    def _initialise_shadow_workflows(self, observation, cluster):
        """
        Use the SHADOW library workflow model to build the graph

        Parameters
        ----------
        observation

        Returns
        -------
        workflow : shadow.models.workflow.Worflow
            A wrapper for NetworkX DiGraph object with additional information

        """
        workflow = Workflow(observation.workflow)
        available_resources = self._cluster_to_shadow_format(cluster)
        workflow_env = Environment(available_resources, dictionary=True)
        workflow.add_environment(workflow_env)
        return workflow

    def _run_scheduling(self, workflow):
        """
        Produce static schedules based on the algorithm specified at
        object creation.

        Returns
        -------
        solution : shadow.model.solution.Solution
            A solution object which describes a static schedule with
            additional information.
        """

        if self.algorithm == 'heft':
            solution = heft(workflow)
        elif self.algorithm == 'pheft':
            solution = pheft(workflow)
        elif self.algorithm == 'fcfs':
            solution = fcfs(workflow)
        else:
            raise RuntimeError(
                f"{self.algorithm} is not implemented by {str(self)}"
            )
        LOGGER.debug(
            "Solution makespan for {0} is {1}".format(
                self.algorithm, solution.makespan
            )
        )
        return solution

    def _cluster_to_shadow_format(self, cluster):
        """
        Given the cluster, select from the available resources to allocate
        and create a dictionary in the format required for shadow.
        Returns
        ----
        dictionary : dict
            dictionary of machine requirements
        """

        # TODO we have reverted to the entire list of machines; can we
        #  improve this moving forward?
        # TODO entire machines
        available_resources = cluster.machines
        dictionary = {
            "system": {
                "resources": None,
                "system_bandwidth": cluster.system_bandwidth
            }
        }
        resources = {}
        for m in available_resources:
            resources[m.id] = {
                "flops": m.cpu,
                "compute_bandwidth": m.bandwidth,
                "io": m.disk,
                "memory": m.memory
            }
        dictionary['system']['resources'] = resources

        return dictionary

