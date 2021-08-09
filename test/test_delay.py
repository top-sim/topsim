# Copyright (C) 16/2/21 RW Bunney

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
Unittests for the topsim.core.delay.DelayModel class
"""

import unittest
import simpy

from topsim.core.config import Config
from topsim.core.scheduler import Scheduler, ScheduleStatus
from topsim.core.cluster import Cluster
from topsim.core.planner import Planner
from topsim.core.buffer import Buffer
from topsim.core.delay import DelayModel

from topsim.user.telescope import Telescope
from topsim.user.schedule.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.plan.static_planning import SHADOWPlanning

INTEGRATION = "test/data/config_update/integration_simulation.json"
PLANNING_ALGORITHM = 'heft'


class TestDelayCreation(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_initialisation(self):

        dm = DelayModel(0.1, "normal")
        self.assertEqual('normal', dm.dist)
        self.assertEqual(0.1, dm.prob)
        # Non-existent/not implemneted distribution defaults to uniform
        dm = DelayModel(0.1, "pink_panther")
        self.assertEqual('uniform', dm.dist)

    def test_random_variable_generation(self):
        # Create a sample task runtime
        dm = DelayModel(0.1, "normal")
        # Ensure default seed is the same as our expectations
        self.assertEqual(20, dm.seed)
        rt = 10
        var = dm._create_random_value_from_runtime(rt)
        self.assertAlmostEqual(11.30,var,2)

    def test_delay_generation(self):
        dm = DelayModel(0.1, "normal")
        rt = 10
        delay = dm.generate_delay(rt)
        # The probability is too low with the given seed, so we don't have a
        # delay
        self.assertEqual(0, delay-rt)
        dm = DelayModel(0.3, "normal")
        delay = dm.generate_delay(rt)
        self.assertEqual(1, delay-rt)


class TestDelaysInActors(unittest.TestCase):

    def setUp(self):
        """
        Repeating above test cases but with delays to determine that delay
        flags reach us.
        Returns
        -------

        """

        self.env = simpy.Environment()
        config = Config(INTEGRATION)
        self.cluster = Cluster(self.env, config)
        self.buffer = Buffer(self.env, self.cluster, config)
        dm = DelayModel(0.9, "normal",
                   DelayModel.DelayDegree.HIGH)
        self.planner = Planner(
            self.env, PLANNING_ALGORITHM,
            self.cluster, SHADOWPlanning('heft',delay_model=dm), delay_model=dm
        )

        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, DynamicAlgorithmFromPlan()
        )
        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        self.scheduler.start()
        self.env.process(self.scheduler.run())
        self.env.process(self.telescope.run())

    def test_scheduler_delay_detection(self):
        """
        Nothing should change until we reach the workflow plan, as we are
        testing TaskDelays
        Returns
        -------
        """

        self.env.run(until=1)
        # Remember - env starts at 0, we don't start until 1.
        self.assertEqual(10, len(self.cluster.resources['available']))
        self.env.run(until=2)

        # After 1 timestep, data in the HotBuffer should be 4
        self.assertEqual(496, self.buffer.hot[0].current_capacity)
        self.env.run(until=31)
        self.assertEqual(5, len(self.cluster.tasks['finished']))
        self.assertEqual(500, self.buffer.hot[0].current_capacity)
        self.env.run(until=44)
        # We know that the schedule has been delayed - however, we don't
        # report this to the telescope until we know how long we are delayed
        # (that is, until the task has completely finished its duration).
        # In this instance. we know that the first task is going to be
        # delayed, and so wait until it's completed execution to trigger a
        # delay.
        self.assertEqual(ScheduleStatus.ONTIME, self.scheduler.schedule_status)
        self.env.run(until=45)
        self.assertTrue(ScheduleStatus.DELAYED,self.scheduler.schedule_status)
        self.env.run(until=124)
        # Assert that we still have tasks running
        # self.assertLess(
        #     0, len(self.cluster.clusters['default']['tasks']['running'])
        # )
        self.assertNotEqual(250, self.buffer.cold[0].current_capacity)

    def test_telescope_delay_detection(self):
        """

        Returns
        -------

        """
        self.env.run(until=1)
        # Remember - env starts at 0, we don't start until 1.
        self.assertEqual(10, len(self.cluster.resources['available']))
        self.env.run(until=2)

        # After 1 timestep, data in the HotBuffer should be 4
        self.assertEqual(496, self.buffer.hot[0].current_capacity)
        self.env.run(until=31)
        self.assertEqual(5, len(self.cluster.tasks['finished']))
        self.assertEqual(500, self.buffer.hot[0].current_capacity)
        self.env.run(until=32)
        # Ensure the time
        self.assertEqual(ScheduleStatus.ONTIME, self.scheduler.schedule_status)
        self.env.run(until=100)
        self.assertEqual(ScheduleStatus.DELAYED,self.scheduler.schedule_status)
        self.assertTrue(self.telescope.delayed)

    def test_telescope_delay_greedy_decision(self):
        """
        Returns
        -------
        """
