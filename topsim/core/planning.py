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
from topsim.core.planner import WorkflowPlan


class Planning(ABC):
    """
    Base class for all planning models, used by the planner actor

    Parameters
    ----------
    observation : topsim.core.instrument.Observation
        Contains workflow associated with the observation processing.
    algorithm : str
         Name of the algorithm used in the model; some models allow for
         multiple algorithms in addition to the model
    buffer : topsim.core.buffer.Buffer
        The simulation buffer object; used for start-time estimation of
        observation

    Attributes
    ----------

    """

    def __init__(self, observation, algorithm, buffer, delay_model=None):
        self.observation = observation
        self.algorithm = algorithm
        self.buffer = buffer
        self.delay_model = delay_model

    @abstractmethod
    def generate_plan(self, clock):
        """
        Build a WorkflowPlan object storing
        Returns
        -------

        """
        plan = None
        return plan


    @abstractmethod
    def to_df(self):
        """

        Returns
        -------
        df : pandas.DataFrame
        """

    def _calc_workflow_est(self):
        """
        Calculate the estimated start time of the workflow based on data
        transfer delays post-observation

        Parameters
        ----------
        Returns
        -------

        """
        storage = self.buffer.buffer_storage_summary()
        size = self.observation.duration * self.observation.ingest_data_rate
        hot_to_cold_time = int(size/storage['coldbuffer']['data_rate'])
        est = self.observation.duration + hot_to_cold_time
        return est

    def _create_observation_task_id(self, tid, clock):
        return self.observation.name + '_' + str(clock) + '_' + str(tid)
