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

from core.simulation import Simulation
from scheduler.fifo_algorithm import FifoAlgorithm

from core.scheduler import Scheduler, Task
from core.cluster import Cluster
from core.planner import Planner

import config_data

class TestSchedulerRandom(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass


class TestSchedulerFIFO(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		sched_algorithm = FifoAlgorithm()
		self.planner = Planner(self.env, config_data.planning_algorithm, config_data.machine_config)
		self.cluster = Cluster(config_data.machine_config)
		self.scheduler = Scheduler(self.env, sched_algorithm, self.cluster)

	def tearDown(self):
		pass

	"""
	The scheduler should be initialised with no workflow plans and valid pairs. 
	"""
	def testSchedulerInit(self):

		pass

	def testSchedulerDecision(self):
		""" scheduler.make_decision() will do something interesting only when we add a workflow plan to the
		cluster resource.
		"""
		# Add workflow plan to Cluster
		plan = Plan(wf,none, none, none)

		pass
