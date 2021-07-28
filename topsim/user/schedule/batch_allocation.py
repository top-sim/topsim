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

from topsim.core.algorithm import Algorithm


class BatchProcessing(Algorithm):
    """
    Dynamic schedule for a workflowplan that is using a batch-processing
    resource reservation approach without generating a static schedule.
    """
    def __init__(self, max_resource_split=2):
        super().__init__()
        self.max_resources_split = max_resource_split
        
    def __repr__(self):
        return "BatchProcessing"

    def run(self, cluster, clock, workflow_plan, existing_schedule):
        """
        """
        provision = self._max_resource_provision(cluster)
        cluster.provision_batch_resources(
            provision, workflow_plan.id
        )

        pass

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
