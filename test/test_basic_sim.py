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
import os
import unittest
import logging
import simpy
from pathlib import Path
from topsim.core.simulation import Simulation
from topsim.core.instrument import RunStatus

from topsim.user.schedule.dynamic_plan import DynamicSchedulingFromPlan
from topsim.user.telescope import Telescope
from topsim.user.plan.static_planning import SHADOWPlanning

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

SIM_TIMESTAMP = f'test/basic-workflow-data/{0}'
BASIC_CONFIG = Path('test/basic-workflow-data/basic_simulation.json')
planning_model = SHADOWPlanning

cwd = os.getcwd()


class TestBasicIngest(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        self.simulation = Simulation(
            self.env,
            BASIC_CONFIG,
            Telescope,
            planning_model=SHADOWPlanning('heft'),
            scheduling=DynamicSchedulingFromPlan(min_resources_per_workflow=1,
                                                 ignore_ingest=True),
            delay=None,
            timestamp=0
        )


    def testClusterIngest(self):
        """
        The basic ingest represents the edge cases for timing and scheduling
        within the simulation, as demonstrated in this test.

        Returns
        -------

        """
        self.assertEqual(0, self.env.now)
        self.simulation.start()
        self.assertEqual(
            3, self.simulation.cluster._ingest['completed']
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
            0, self.simulation.buffer.hot[0].current_capacity
        )
        self.assertEqual(
            10, self.simulation.buffer.cold[0].current_capacity
        )
        self.assertEqual(
            0,
            len(self.simulation.buffer.hot[0].observations["scheduled"])
        )
        self.simulation.resume(until=10)
        self.assertEqual(5, self.simulation.buffer.hot[0].current_capacity)
        self.assertEqual(0, self.simulation.buffer.cold[0].current_capacity)
        self.assertEqual(
            2,
            len(self.simulation.buffer.cold[0].observations["stored"])
        )
        self.simulation.resume(until=16)
        self.assertEqual(5, self.simulation.buffer.hot[0].current_capacity)
        self.assertEqual(5, self.simulation.buffer.cold[0].current_capacity)

    def testSchedulerRunTime(self):
        self.assertEqual(0, self.simulation.env.now)
        self.simulation.start(runtime=2)
        self.assertEqual(
            0, len(self.simulation.buffer.hot[0].observations['scheduled'])
        )
        self.assertEqual(
            0, len(self.simulation.buffer.cold[0].observations['stored'])
        )
        self.simulation.resume(until=16)
        # We've finished processing one of the workflows so one observation
        # is finished.
        self.assertEqual(
            1, len(self.simulation.buffer.hot[0].observations['finished'])
        )
