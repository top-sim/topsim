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
    Base class for all planning models, used by the planner actor

    Parameters
    ----------
    observation : ~topsim.core.instrument.Observation
        Contains workflow associated with the observation processing.
    algorithm : str
         Name of the algorithm used in the model; some models allow for
         multiple algorithms in addition to the model

    delay_model:

    Attributes
    ----------

    """

    def __init__(self, algorithm: str, delay_model=None):
        # self.observation = observation
        self.algorithm = algorithm
        self.delay_model = delay_model

    @abstractmethod
    def generate_plan(self, clock, cluser, buffer, observation, max_ingest):
        """
        Build a WorkflowPlan object storing
        Returns
        -------
            plan : core.topsim.planner.WorksplatflowPlan
            WorkflowPlan object
        """
        pass

    @abstractmethod
    def to_df(self):
        """
        Generate output to be amalgamated into the global simulation data
        frame produced by the Monitor

        Returns
        -------
        df : pandas.DataFrame
        """
        
        pass

    def _calc_workflow_est(self,observation, buffer):
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
        est = observation.duration + hot_to_cold_time
        return est

    def _create_observation_task_id(self, tid, observation, clock):
        return observation.name + '_' + str(clock) + '_' + str(tid)
