# Copyright (C) 8/9/20 RW Bunney

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
import os

from topsim.core.task import Task
from topsim.core.cluster import Cluster

CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"


class TestTaskInit(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		self.cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)

	def test_task_setup(self):

		task = Task(0, env=self.env)