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

"""
Abstract class for building a workflow plan

"""

from abc import ABC, abstractmethod
from topsim.core.planner import Planner, WorkflowPlan


class Planning(ABC):
    """
    Abstract base class for planning models.

    A planning model generates a :py:class:`~topsim.core.planner.WorkflowPlan`
    for an observation, producing a static schedule or task ordering that
    the Scheduler will later execute dynamically.

    Parameters
    ----------
    algorithm : str
        Name of the algorithm used by this model (e.g. ``'heft'``,
        ``'batch'``).
    delay_model : ~topsim.core.delay.DelayModel, optional
        Delay model to attach to generated tasks.

    Attributes
    ----------
    algorithm : str
    delay_model : DelayModel or None
    """

    def __init__(self, algorithm: str, delay_model=None):
        self.algorithm = algorithm
        self.delay_model = delay_model

    @abstractmethod
    def to_string(self):
        """
        Return the string name of this planning implementation.
        """

    @abstractmethod
    def generate_plan(self, clock, cluster, buffer, observation, max_ingest,
                      task_data=False, edge_data=True):
        """
        Build a WorkflowPlan for the given observation.

        Parameters
        ----------
        clock : int
            Current simulation time.
        cluster : topsim.core.cluster.Cluster
            The Cluster actor (for resource information).
        buffer : topsim.core.buffer.Buffer
            The Buffer actor (for storage state).
        observation : topsim.core.instrument.Observation
            The observation to plan for.
        max_ingest : int
            Maximum ingest resources.
        task_data : bool, optional
            Whether to include per-task data in planning.
        edge_data : bool, optional
            Whether to include edge data transfer costs.

        Returns
        -------
        WorkflowPlan
            The generated workflow plan.
        """

    @abstractmethod
    def to_df(self):
        """
        Generate output for the Monitor.

        Returns
        -------
        pandas.DataFrame
        """
        

    def _calc_workflow_est(self, observation, buffer):
        """
        Calculate the estimated start time of the workflow based on data
        transfer delays post-observation

        Parameters
        ----------
        Returns
        -------

        """
        storage = buffer.buffer_storage_summary()
        size = observation.duration * observation.ingest_data_rate
        hot_to_cold_time = int(size/storage['coldbuffer']['data_rate'])
        est = observation.duration # + hot_to_cold_time
        return est

    def _create_observation_task_id(self, tid, observation, clock):
        return observation.name + '_' + str(clock) + '_' + str(tid)
