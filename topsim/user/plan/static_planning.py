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

from topsim.core.planning import Planning
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

    def __init__(self, observation, algorithm, buffer, delay_model=None):

        super().__init__(observation, algorithm, buffer)
        # self.observation = observation
        self.algorithm = algorithm
        # self.buffer = buffer
        self.delay_model = delay_model

    def generate_plan(self, clock):
        """
        For this StaticPlanning example, we are using the SHADOW static
        scheduling library to produce static plans. There are a couple of

        Parameters
        ----------
        clock : int
            Current simulation time (usually provided through `env.now`)

        Returns
        -------

        """
        if self.observation.ast is None:
            raise RuntimeError(
                f'Observation AST must be updated before plan'
            )
        solution = self._run_scheduling()

        est = self._calc_workflow_est()
        eft = solution.makespan
        tasks = []

        for task in solution.task_allocations:
            allocation = solution.task_allocations.get(task)
            tid = self._create_observation_task_id(task.tid)
            dm = copy.copy(self.delay_model)
            predecessors = [
                self._create_observation_task_id(x.tid) for x in list(
                    solution.workflow.graph.predecessors(task)
                )
            ]
            edge_costs = {}
            data = dict(solution.workflow.graph.pred[task])
            for element in data:
                nm = self._create_observation_task_id(element.tid, clock)
                val = data[element]['data_size']
                edge_costs[nm]=val
            taskobj = Task(
                tid,
                allocation.ast,
                allocation.aft,
                allocation.machine.id,
                predecessors,
                task.flops_demand, 0, edge_costs,
                dm
            )
            tasks.append(taskobj)
        tasks.sort(key=lambda x: x.est)
        exec_order = solution.execution_order

        return WorkflowPlan(
            est, eft, tasks, exec_order, WorkflowStatus.SCHEDULED
        )

    def to_df(self):
        pass

    def _run_scheduling(self):
        """
        Produce static schedules based on the algorithm specified at
        object creation.

        Returns
        -------
        solution : shadow.model.solution.Solution
            A solution object which describes a static schedule with
            additional information.
        """
        workflow = Workflow(self.observation.workflow)
        available_resources = self._cluster_to_shadow_format()
        workflow_env = Environment(available_resources, dictionary=True)
        workflow.add_environment(workflow_env)

        if self.algorithm is 'heft':
            solution = heft(workflow)
        elif self.algorithm is 'pheft':
            solution = pheft(workflow)
        elif self.algorithm is 'fcfs':
            solution = fcfs(workflow)
        else:
            raise RuntimeError(
                f"{self.algorithm} is not implemented by {str(self)}"
            )
        LOGGER.debug(
            "Solution makespan for {0} is {1}".format(
                self.algorithm, self.solution.makespan
            )
        )
        return solution

    def _cluster_to_shadow_format(self):
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
        available_resources = list(self.cluster.dmachine.values())
        dictionary = {
            "system": {
                "resources": None,
                "bandwidth": self.cluster.system_bandwidth
            }
        }
        resources = {}
        for m in available_resources:
            resources[m.id] = {
                "flops": m.cpu,
                "rates": m.bandwidth,
                "io": m.disk,
                "memory": m.memory
            }
        dictionary['system']['resources'] = resources

        return dictionary


"""
"""