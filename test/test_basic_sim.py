# Copyright (C) 6/11/20 RW Bunney

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
Test an entire run-through simulation to make sure all the 'bits' fit in place
"""

import unittest
import logging
import simpy
from topsim.core.simulation import Simulation
from topsim.core.instrument import RunStatus

from topsim.user.scheduling import FifoAlgorithm
from topsim.user.telescope import Telescope

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

BASIC_WORKFLOW = 'test/basic-workflow-data/basic_workflow_config.json'
BASIC_CLUSTER = 'test/basic-workflow-data/basic_config.json'
BASIC_BUFFER = 'test/basic-workflow-data/basic_buffer.json'
BASIC_PLAN = 'test/basic-workflow-data/basic_observation_plan.json'
BASIC_CONFIG = 'test/basic-workflow-data/basic_simulation.json'


class TestBasicIngest(unittest.TestCase):

    def setUp(self) -> None:
        event_file = EVENT_FILE
        self.env = simpy.Environment()
        planning_algorithm = 'heft'
        algorithm_map = {'heft':'heft', 'fifo': FifoAlgorithm}
        self.simulation = Simulation(
            self.env,
            BASIC_CONFIG,
            Telescope,
            algorithm_map,
        )

    def testClusterIngest(self):
        """
        The basic ingest represents the edge cases for timing and scheduling
        within the simulation, as demonstrated in this test.

        There are a couple of edge cases that occur here, especially when we
        consider that we have only 2 resources; one of these will be taken by
        ingest, meaning that we cannot start an observation until 1 timestep
        AFTER an ingest has finished, because the telescope will check before
        that task is successfully removed from the cluster.

        This is why we run for 6 seconds and only process 2 observations.

        After we've observed 2 observations, we reach capacity on the
        cold-buffer so we are unable to observe any more.

        Returns
        -------

        """
        self.assertEqual(0, self.env.now)
        self.simulation.start(runtime=7)
        self.assertEqual(
            2, self.simulation.cluster.ingest['completed']
        )

        self.assertEqual(
            RunStatus.FINISHED,
            self.simulation.instrument.observations[1].status
        )

    def testBufferIngest(self):
        self.assertEqual(0, self.simulation.env.now)
        self.simulation.start(runtime=1)
        self.assertEqual(
            5, self.simulation.buffer.hot[0].current_capacity
        )
        self.simulation.resume(until=2)
        self.assertEqual(
            10, self.simulation.buffer.hot[0].current_capacity
        )
        self.assertEqual(
            5, self.simulation.buffer.cold[0].current_capacity
        )
        self.assertEqual(
            1,
            len(self.simulation.buffer.cold[0].observations["stored"])
        )
        self.simulation.resume(until=4)
        self.assertEqual(10, self.simulation.buffer.hot[0].current_capacity)
        self.assertEqual(0, self.simulation.buffer.cold[0].current_capacity)
        self.assertEqual(
            2,
            len(self.simulation.buffer.cold[0].observations["stored"])
        )

    def testSchedulerRunTime(self):
        self.assertEqual(0, self.simulation.env.now)
        self.simulation.start(runtime=2)
        self.assertEqual(
            1, len(self.simulation.buffer.cold[0].observations['stored'])
        )
        self.simulation.resume(until=8)
        self.simulation.resume(until=11)
        self.simulation.resume(until=12)
        # self.assertEqual(0, len(self.simulation.cluster.tasks['running']))
        # We've finished processing one of the workflows so one observation
        # is finished.
        self.assertEqual(
            1, len(self.simulation.buffer.cold[0].observations['stored'])
        )

