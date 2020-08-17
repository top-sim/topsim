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
import logging
import json
import simpy

from core.cluster import Cluster
from core.telescope import Observation
from core.planner import TaskStatus

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"
CLUSTER_NOFILE = "test/data/config/cluster_config.json"  # Does not exist
CLUSTER_INCORRECT_JSON = "test/data/config/sneaky.json"
CLUSTER_NOT_JSON = "test/data/config/oops.txt"

OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_WORKFLOW = "test/data/config/workflow_config.json"


class TestClusterConfig(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()

	def testClusterConfigFileExists(self):
		"""
		Initialise a Cluster object with the machine config file
		"""
		cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)
		# This is a homogeneous file, so each flops value should be 95
		for machine in cluster.machines:
			# machine is an object instance of Machine
			self.assertEqual(84, machine.cpu)
			self.assertEqual(10, machine.bandwidth)

	def testClusterConfigNoFile(self):
		"""
		Attempt to initialise a cluster with the wrong file
		:return: None
		"""
		config = CLUSTER_NOFILE
		self.assertRaises(
			FileNotFoundError, Cluster, self.env, config
		)

	def testClusterConfigIncorrectJSON(self):
		config = CLUSTER_INCORRECT_JSON
		self.assertRaises(
			KeyError, Cluster, self.env, config
		)

	def testClusterConfigNotJSON(self):
		config = CLUSTER_NOT_JSON
		self.assertRaises(
			json.JSONDecodeError, Cluster, self.env, config
		)


class TestIngest(unittest.TestCase):

	def setUp(self) -> None:
		self.env = simpy.Environment()
		self.cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)
		self.observation = Observation(
			'planner_observation',
			OBS_START_TME,
			OBS_DURATION,
			OBS_DEMAND,
			OBS_WORKFLOW,
			type=None,
			data_rate=None
		)

	def testClusterCheckIngest(self):
		retval = self.cluster.check_ingest_capacity(pipeline_demand=5)
		self.assertTrue(retval)

	def testClusterProvisionIngest(self):
		duration = self.observation.duration
		pipeline_demand = 5

		self.env.process(self.run_ingest(duration,pipeline_demand))
		# self.env.run(until=20)

	def run_ingest(self, duration,demand):
		retval = self.cluster.provision_ingest_resources(
			duration,
			demand
		)
		for task in self.cluster.running_tasks:
			self.assertEqual(TaskStatus.SCHEDULED, task.task_status)
		yield self.env.timeout(3)
		for task in self.cluster.running_tasks:
			self.assertEqual(task.task_status, TaskStatus.RUNNING)


class TestCluster(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def testClusterQueue(self):
		env = simpy.Environment()
		env.process(self.run_env(env))
		env.run(until=20)

	def run_env(self, env):
		i = 0
		while i < 15:
			logger.debug(env.now)
			if env.now == 5:
				env.process(self.secret(env))
			i += 1
			yield env.timeout(1)

	def secret(self, env):
		logger.debug('Started secret business @ {0}'.format(env.now))
		yield env.timeout(4)
		logger.debug('Finished secret business @ {0}'.format(env.now))
