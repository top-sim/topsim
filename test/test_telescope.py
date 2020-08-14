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
import json

from core.telescope import Telescope
from core.scheduler import Scheduler
from core.buffer import Buffer
from core.cluster import Cluster
from core.planner import Planner

OBSERVATION_CONFIG = 'test/data/config/observations.json'
CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"
BUFFER_CONFIG = 'test/data/config/buffer.json'


class TestTelescopeConfig(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)
		buffer = Buffer(env=self.env,cluster=cluster, config=BUFFER_CONFIG)
		self.scheduler = Scheduler(
			env=self.env, buffer=buffer,cluster=cluster, algorithm=None
		)
		planner = Planner(self.env, 'heft', cluster)

	def testTelescopeBasicConfig(self):
		telescope = Telescope(
			env=self.env, config=OBSERVATION_CONFIG,
			planner=None, scheduler=self.scheduler
		)
		self.assertEqual(36, telescope.total_arrays)
		self.assertEqual(10, telescope.pipelines['pulsar'])


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


