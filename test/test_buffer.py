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

from common import data as test_data
from core.planner import Planner
from core.telescope import Telescope, Observation
from core.buffer import Buffer
from core.cluster import Cluster

# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_WORKFLOW = test_data.test_buffer_workflow
MACHINE_CONFIG = test_data.machine_config
PLAN_ALGORITHM = test_data.planning_algorithm

BUFFER_CONFIG  = 'path/to/buffer/config/file'

class TestBufferConfig(unittest.TestCase):

	def setUp(self):
		self.config = BUFFER_CONFIG

	def testHotBufferConfig(self):
		"""
		Process the Hot Buffer section of the config file
		"""

	def testColdBufferConfig(self):
		"""
		Process cold buffer section of the config file
		:return:
		"""


class TestBufferIngestDataStream(unittest.TestCase):

	def setUp(self):
		"""
		setup the buffer and do config stuff
		:return: Nothing
		"""
		self.env = simpy.Environment()
		self.cluster = Cluster(self.env, MACHINE_CONFIG)
		self.buffer = Buffer(self.env, self.cluster)
		self.observation = Observation('scheduler_observation',
									   OBS_START_TME,
									   OBS_DURATION,
									   OBS_DEMAND,
									   OBS_WORKFLOW)

	def testIngestEdgeCase(self):
		"""
		Buffer must accept ingest at rate up to 'max ingest data rate' but
		raises an exception if the ingest rate for an observation is greater
		(this means we have an error).

		In addition, we are coordinating this ingest between the scheduler and
		the telescope and the cluster so these actors also need to work
		together in some way, which this test will also attempt to do .

		:return: No return value as this is a test :'(
		"""
		telescope = Telescope(
			self.env,
			self.observation,
			self.buffer,
			telescope_config=None,
			planner=None
		)

		# test what happens when there is no ingest pipeline on cluster
		original_capacity = None
		next(self.buffer.run())
		self.assertEqual(self.buffer.hot.capacity, original_capacity)

class TestColdBufferWorkflowStream(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		self.cluster = Cluster(self.env, MACHINE_CONFIG)
		self.buffer = Buffer(self.env, self.cluster)
		self.observation = Observation('scheduler_observation',
									   OBS_START_TME,
									   OBS_DURATION,
									   OBS_DEMAND,
									   OBS_WORKFLOW)

		self.planner = Planner(self.env, PLAN_ALGORITHM, MACHINE_CONFIG)

	def tearDown(self):
		pass

	def testWorkflowAddedToQueue(self):
		"""
		We only add a workflow to the queue once an observation has finished
		(and, therefore, after we have finished generating a plan for it).
		:return: None
		"""

		# Calling planner.run() will store the generate plan in the observation object
		# calling next() runs the iterator immediately after generator is called
		next(self.planner.run(self.observation))
		# Buffer observation queue should be empty
		self.assertTrue(self.buffer.observations_for_processing.empty())
		self.buffer.add_observation_to_waiting_workflows(self.observation)
		self.assertTrue(self.buffer.observations_for_processing.size() == 1)
#
# # Get the observation and check we have applied the buffer offset
# self.assertTrue(self.observation.start > OBS_START_TME + OBS_DURATION)
