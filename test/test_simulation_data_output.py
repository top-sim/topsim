# Copyright (C) 25/3/21 RW Bunney

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

import unittest
import simpy
import logging

from topsim.core.simulation import Simulation
from topsim.user.scheduling import FifoAlgorithm
from topsim.user.telescope import Telescope

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

CONFIG = "test/data/simulation_pickles/standard_simulation.json"
INTEGRATION = "test/data/simulation_pickles/integration_simulation.json"
HEFT_CONFIG = "test/data/simulation_pickle/heft_single_observation_simulation" \
              ".json"

# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_WORKFLOW = "test/data/config/workflow_config.json"
PLANNING_ALGORITHM = 'heft'
EVENT_FILE = 'test/simulation_pickles/heft_sim_delay_low'


class TestMonitorPandasPickle(unittest.TestCase):

    def setUp(self) -> None:
        """
        Basic simulation using a single observation + heft workflow +
        homogenous system configuration.
        Returns
        -------
        """
        env = simpy.Environment()
        sched_algorithm = FifoAlgorithm()
        instrument = Telescope
        simulation = Simulation(
            env=env,
            config=CONFIG,
            instrument=instrument,
            algorithm_map={'pheft': 'pheft', 'heft': 'heft',
                           'fifo': FifoAlgorithm},
            event_file=EVENT_FILE,
        )

    def testPickleGeneratedAfterSimulation(self):

        pass

    def testPickleResultsAgreeWithExpectations(self):
        pass

    def testPickleResultsWithLowDegreeDelays(self):
        pass