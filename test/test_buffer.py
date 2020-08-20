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
import json

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

CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"
BUFFER_CONFIG = 'test/data/config/buffer.json'
BUFFER_NOFILE = "test/data/config/buffer_config.json"  # Does not exist
BUFFER_INCORRECT_JSON = "test/data/config/sneaky.json"
BUFFER_NOT_JSON = "test/data/config/oops.txt"


class TestBufferConfig(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		self.cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)

	def testHotBufferConfig(self):
		"""
		Process the Hot Buffer section of the config file
		"""
		buffer = Buffer(
			env=self.env, cluster=self.cluster, config=BUFFER_CONFIG
		)
		self.assertEqual(500, buffer.hot.total_capacity)
		self.assertEqual(500, buffer.hot.current_capacity)
		self.assertEqual(5, buffer.hot.max_ingest_data_rate)

	def testColdBufferConfig(self):
		"""
		Process cold buffer section of the config file
		:return:
		"""
		buffer = Buffer(
			env=self.env, cluster=self.cluster, config=BUFFER_CONFIG
		)
		self.assertEqual(250, buffer.cold.total_capacity)
		self.assertEqual(250, buffer.cold.current_capacity)
		self.assertEqual(2, buffer.cold.max_data_rate)

	def testBufferConfigNoFile(self):
		"""
		Attempt to initialise a cluster with the wrong file
		:return: None
		"""
		config = BUFFER_NOFILE
		self.assertRaises(
			FileNotFoundError, Buffer, self.env, self.cluster, config
		)

	def testBufferConfigIncorrectJSON(self):
		config = BUFFER_INCORRECT_JSON
		self.assertRaises(
			KeyError, Buffer, self.env, self.cluster, config
		)

	def testBufferConfigNotJSON(self):
		config = BUFFER_NOT_JSON
		self.assertRaises(
			json.JSONDecodeError, Buffer, self.env, self.cluster, config
		)


class TestBufferIngestDataStream(unittest.TestCase):

	def setUp(self):
		"""

		Returns
		-------

		"""
		"""
		setup the buffer and do config stuff
		:return: Nothing
		"""
		self.env = simpy.Environment()
		self.cluster = Cluster(self.env, MACHINE_CONFIG)
		self.buffer = Buffer(self.env, self.cluster, BUFFER_CONFIG)
		self.observation = Observation(
			name='test_observation',
			start=OBS_START_TME,
			duration=OBS_DURATION,
			demand=OBS_DEMAND,
			workflow=OBS_WORKFLOW,
			type='continuum',
			data_rate=5,
		)

	def testBasicIngest(self):
		"""
		Test the 'ingest_data_stream' event, which is to be called in the
		Scheduler. The changes we expect in this are simple - after n
		timesteps, the HotBuffer.currect_capacity will have reduced
		n*observation_ingest_rate.
		"""

		ret = self.env.process(
			self.buffer.ingest_data_stream(
				self.observation
			)
		)

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

		# test what happens when there is no ingest pipeline on cluster

		original_capacity = None
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
