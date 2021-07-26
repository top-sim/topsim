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
import networkx as nx

from topsim.core.planning import Planning


class BatchPlanning(Planning):
    """
    Create a placeholder, topologically sorted plan for the batch-scheduler
    and call cluster.provision_batch_resources, updating the state of
    the cluster.

    """

    def __init__(self, algorithm, delay_model=None, max_resource_split=2):
        super().__init__(algorithm, delay_model)
        self.max_resources_split = max_resource_split

    def __str__(self):
        return

    def generate_plan(self, clock, cluster, buffer, observation):
        """

        Parameters
        ----------
        clock
        buffer
        observation
        """
        plan = None
        if self.algorithm is 'batch':
            workflow = self._workflow_to_nx(observation.workflow)
            provision = self._max_resource_provision(cluster)
            cluster.provision_batch_resources(
                provision, observation
            )

        else:
            raise RuntimeError(
                f'{self.algorithm} is not supported by {str(self)}'
            )

        return plan

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

    def _max_resource_provision(self, cluster):
        """

        Calculate the appropriate number of resources to provision accordingly

        Parameters
        ----------
        n_resources : int
            The total number of resources available to the graph

        Notes
        -----

        Returns
        -------
        prov_resources : int
            Number of resources to provision based on our provisioning heuristic
        """
        available = len(cluster.get_available_resources())
        # Ensure we don't provision more than is acceptable for a single
        # workflow
        max_allowed = int(len(cluster.dmachine) / self.max_resources_split)
        if available == 0:
            return None
        if available < max_allowed:
            return available
        else:
            return max_allowed

    def to_df(self):
        pass
