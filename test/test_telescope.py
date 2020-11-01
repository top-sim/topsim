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

from topsim.core.telescope import Telescope
from topsim.core.scheduler import Scheduler
from topsim.core.buffer import Buffer
from topsim.core.cluster import Cluster
from topsim.core.planner import Planner

OBSERVATION_CONFIG = 'test/data/config/observations.json'
CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"
BUFFER_CONFIG = 'test/data/config/buffer.json'


class TestTelescopeConfig(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)
        buffer = Buffer(env=self.env, cluster=cluster, config=BUFFER_CONFIG)
        self.scheduler = Scheduler(
            env=self.env, buffer=buffer, cluster=cluster, algorithm=None
        )
        planner = Planner(self.env, 'heft', cluster)

    def testTelescopeBasicConfig(self):
        telescope = Telescope(
            env=self.env, config=OBSERVATION_CONFIG,
            planner=None, scheduler=self.scheduler
        )
        self.assertEqual(36, telescope.total_arrays)
        self.assertEqual(5, telescope.pipelines['pulsar']['demand'])


class TestTelescopeIngest(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        self.cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)
        self.buffer = Buffer(env=self.env, cluster=self.cluster,
                             config=BUFFER_CONFIG)
        self.scheduler = Scheduler(
            env=self.env, buffer=self.buffer, cluster=self.cluster,
            algorithm=None
        )
        self.planner = Planner(self.env, 'heft', self.cluster)

    def testIngest(self):
        telescope = Telescope(
            env=self.env, config=OBSERVATION_CONFIG,
            planner=self.planner, scheduler=self.scheduler
        )
        self.assertEqual(0, telescope.telescope_use)
        self.env.process(telescope.run())
        self.scheduler.init()
        self.env.process(self.scheduler.run())
        self.env.run(until=1)
        self.assertEqual(36, telescope.telescope_use)
        self.assertEqual(5, len(self.cluster.available_resources))
        # After 1 timestep, data in the HotBuffer should be 2
        self.assertEqual(496, self.buffer.hot.current_capacity)
        self.env.run(until=10)
        self.assertEqual(460, self.buffer.hot.current_capacity)
        self.env.run(until=12)
        self.assertEqual(0, telescope.telescope_use)
        self.assertEqual(10, len(self.cluster.available_resources))
        self.assertEqual(5, len(self.cluster.finished_tasks))
        self.assertEqual(1, len(self.buffer.waiting_observation_list))
    # self.assertEqual(1, len(self.scheduler.waiting_observations))


class TestObservationConfig(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()

    def testObservationConfigJSON(self):
        """
        Ensure the JSON has been presented directly

        :return:
        """


class TestTelescope(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
