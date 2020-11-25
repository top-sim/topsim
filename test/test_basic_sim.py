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
from topsim.algorithms.scheduling import FifoAlgorithm

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

EVENT_FILE = 'test/basic-workflow-data/output/sim.trace'
BASIC_WORKFLOW = 'test/basic-workflow-data/basic_workflow_config.json'
BASIC_CLUSTER = 'test/basic-workflow-data/basic_config.json'
BASIC_BUFFER = 'test/basic-workflow-data/basic_buffer.json'
BASIC_PLAN = 'test/basic-workflow-data/basic_observation_plan.json'


class TestBasicIngest(unittest.TestCase):

    def setUp(self) -> None:
        event_file = EVENT_FILE
        self.env = simpy.Environment()
        planning_algorithm = 'heft'
        scheduling_algorithm = FifoAlgorithm()
        self.simulation = Simulation(
            self.env,
            BASIC_PLAN,
            BASIC_CLUSTER,
            BASIC_BUFFER,
            planning_algorithm,
            scheduling_algorithm,
            EVENT_FILE,
        )

    def testClusterIngest(self):
        self.assertEqual(0, self.env.now)
        self.simulation.start(runtime=1)
        self.assertEqual(1, self.simulation.cluster.ingest['completed'])
        self.assertEqual(
            0, len(self.simulation.cluster.resources['ingest'])
        )
        self.simulation.resume(until=3)
        self.assertEqual(
            3, self.simulation.cluster.ingest['completed']
        )
        self.assertEqual(
            3, len(self.simulation.cluster.tasks['finished'])
        )
        # self.simulation.start(runtime=6)
        # self.assertEqual(
        #     2, self.simulation.cluster.ingest['completesd']
        # )
        # self.assertEqual(
        #     self.simulation.telescope.RunStatus.FINISHED,
        #     self.simulation.telescope.observations[2].RunStatus
        # )

    def testBufferIngest(self):
        self.assertEqual(0, self.simulation.env.now)
        self.simulation.start(runtime=1)
        self.assertEqual(
            5, self.simulation.buffer.hot.current_capacity
        )
        self.simulation.resume(until=2)
        self.assertEqual(
            5, self.simulation.buffer.hot.current_capacity
        )
        self.assertEqual(
            5, self.simulation.buffer.cold.current_capacity
        )
        self.assertEqual(
            1,
            len(self.simulation.buffer.hot.observations["stored"])
        )
        self.simulation.resume(until=3)
        self.assertEqual(5, self.simulation.buffer.hot.current_capacity)
        self.assertEqual(0, self.simulation.buffer.cold.current_capacity)
        self.assertEqual(
            1,
            len(self.simulation.buffer.hot.observations["stored"])
        )
        self.assertEqual(
            2,
            len(self.simulation.buffer.cold.observations["stored"])
        )

    def testSchedulerRunTime(self):
        self.assertEqual(0, self.simulation.env.now)
        self.simulation.start(runtime=2)
        self.assertEqual(
            1, len(self.simulation.buffer.cold.observations['stored'])
        )
        self.simulation.resume(until=8)
        self.simulation.resume(until=9)
        self.assertEqual(0, len(self.simulation.cluster.tasks['running']))
        self.simulation.resume(until=10)
        self.simulation.resume(until=11)
        self.assertEqual(
            2, len(self.simulation.buffer.cold.observations['stored'])
        )

