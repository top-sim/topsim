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

import json
import copy
import networkx as nx

from topsim.core.task import Task
from topsim.algorithms.planning import Planning
from topsim.core.planner import WorkflowStatus, WorkflowPlan


class BatchPlanning(Planning):
    """
    Create a placeholder, topologically sorted plan for the batch-scheduler
    and call cluster.provision_batch_resources, updating the state of
    the cluster.

    """

    def __init__(self, algorithm, delay_model=None):
        super().__init__(algorithm, delay_model)

    def __str__(self):
        return 'BatchPlanning'

    def generate_plan(self, clock, cluster, buffer, observation, max_ingest):
        """

        Parameters
        ----------
        cluster
        clock
        buffer
        observation
        """
        plan = None
        if self.algorithm is 'batch':
            graph = self._workflow_to_nx(observation.workflow)
            est = self._calc_workflow_est(observation, buffer)
            # new_graph = nx.DiGraph()
            mapping = {}
            tasks = []
            exec_order = list(nx.algorithms.topological_sort(graph))
            for task in exec_order:
                tid = self._create_observation_task_id(
                    task, observation, clock
                )
                dm = copy.copy(self.delay_model)
                pred = list(graph.predecessors(task))
                predecessors = [
                    self._create_observation_task_id(
                        x, observation, clock
                    ) for x in pred
                ]
                succ = list(graph.successors(task))
                successors = [
                    self._create_observation_task_id(
                        x, observation, clock
                    ) for x in pred
                ]

                # Get the data transfer costs
                edge_costs = {}
                data = dict(graph.pred[task])
                for element in data:
                    nm = self._create_observation_task_id(
                        element, observation, clock
                    )
                    val = data[element]["transfer_data"]
                    edge_costs[nm] = val

                est, eft = 0, 0
                machine_id = None
                task_compute =  graph.nodes[task]['comp']
                task_data = 0
                if 'task_data' in  graph.nodes[task]:
                    task_data = graph.nodes[task]['task_data']

                taskobj = Task(
                    tid, est, eft, machine_id, predecessors, task_compute,
                    task_data, edge_costs, dm, gid=task
                )
                mapping[task] = taskobj
                tasks.append(taskobj)
            new_graph = nx.relabel_nodes(graph, mapping)
            # exec_order = list(nx.algorithms.topological_sort(graph))
            return WorkflowPlan(
                observation.name, est, -1, tasks, exec_order,
                WorkflowStatus.SCHEDULED, max_ingest, new_graph
            )

        else:
            raise RuntimeError(
                f'{self.algorithm} is not supported by {str(self)}'
            )


    def _workflow_to_nx(self, workflow):
        """
        Read workflow file into networkx graph
        Parameters
        ----------
        workflow

        Returns
        -------
        graph : networkx.DiGraph object
        """
        with open(workflow, 'r') as infile:
            config = json.load(infile)
        graph = nx.readwrite.node_link_graph(config['graph'])
        return graph

    def to_df(self):
        pass
