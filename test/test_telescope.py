# Copyright (C) 27/11/19 RW Bunney

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

from topsim.core.config import Config
from topsim.user.telescope import Telescope
from topsim.core.scheduler import Scheduler
from topsim.core.buffer import Buffer
from topsim.core.cluster import Cluster
from topsim.core.planner import Planner
from topsim.user.plan.static_planning import SHADOWPlanning

CONFIG = 'test/data/config/standard_simulation_longtask.json'


class TestTelescopeConfig(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        self.config = Config(CONFIG)
        cluster = Cluster(env=self.env, config=self.config)
        planner = Planner(self.env, cluster, SHADOWPlanning('heft'))
        buffer = Buffer(env=self.env, cluster=cluster, planner=planner, config=self.config)
        self.scheduler = Scheduler(
            env=self.env, buffer=buffer, cluster=cluster, planner=planner, algorithm=None
        )

    def testTelescopeBasicConfig(self):
        telescope = Telescope(
            env=self.env, config=self.config,
            planner=None, scheduler=self.scheduler
        )
        self.assertEqual(36, telescope.total_arrays)
        # Pipelines are associated with individual observation
        self.assertTrue('emu' in telescope.pipelines)
        # self.assertEqual(5, telescope.pipelines['pulsar']['demand'])


class TestTelescopeIngest(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        self.config = Config(CONFIG)

        self.cluster = Cluster(env=self.env, config=self.config)
        self.planner = Planner(self.env, self.cluster,
                               SHADOWPlanning('heft'))
        self.buffer = Buffer(env=self.env, cluster=self.cluster, planner=self.planner,
                             config=self.config)
        self.scheduler = Scheduler(
            env=self.env, buffer=self.buffer, cluster=self.cluster, planner=self.planner,
            algorithm=None
        )

    def testIngest(self):
        telescope = Telescope(
            env=self.env, config=self.config,
            planner=self.planner, scheduler=self.scheduler
        )
        self.assertEqual(0, telescope.telescope_use)
        self.env.process(telescope.run())
        self.env.process(self.cluster.run())
        self.scheduler.start()
        self.env.process(self.scheduler.run())
        self.env.process(self.buffer.run())
        self.env.run(until=2)
        self.assertEqual(36, telescope.telescope_use)
        self.assertEqual(5, len(self.cluster._resources['available']))
        # After 1 timestep, data in the HotBuffer should be 2
        self.assertEqual(492e9, self.buffer.hot[0].current_capacity)
        self.env.run(until=10)
        self.assertEqual(460e9, self.buffer.hot[0].current_capacity)
        # self.assertEqual(
        #     len([self.buffer.hot[0].observations["transfer"]]),
        #     1
        # )
        # self.assertEqual(462e9, self.buffer.hot[0].current_capacity)
        # self.assertEqual(248e9, self.buffer.cold[0].current_capacity)
        # self.env.run(until=12)
        # self.assertEqual(0, telescope.telescope_use)
        # self.assertEqual(10, len(self.cluster._resources['available']))
        # self.assertEqual(5, len(self.cluster._tasks['finished']))


class TestTaskDelayDetection(unittest.TestCase):
    """
    The telescope will flag delays and report this as a flag.
    """


class TestObservationConfig(unittest.TestCase):

    def setUp(self):
        config = Config(CONFIG)
        (
            total_arrays, pipelines, self.observations, max_ingest
        ) = config.parse_instrument_config('telescope')


    def testObservationConfigJSON(self):
        """
        Ensure the JSON has been presented directly

        :return:
        """
       # self.observations


class TestTelescope(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
