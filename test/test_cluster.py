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

from topsim.core.config import Config
from topsim.core.cluster import Cluster
from topsim.core.instrument import Observation
from topsim.core.task import TaskStatus

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

CONFIG = "test/data/config/standard_simulation.json"
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
        config = Config(CONFIG)
        cluster = Cluster(env=self.env, config=config)
        # This is a homogeneous file, so each flops value should be 95
        for machine in cluster.machines:
            # machine is an object instance of Machine
            self.assertEqual(84, machine.cpu)
            self.assertEqual(10, machine.bandwidth)


class TestIngest(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        config = Config(CONFIG)

        self.cluster = Cluster(env=self.env, config=config)
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
        """
        Need to determine that the cluster is able to deal with the pipeline
        requirement

        in addition, need to check that we are not going over the maximum
        number of resources that can be feasibly allocated to ingest.
        Returns
        -------

        """
        max_ingest = 5
        retval = self.cluster.check_ingest_capacity(5, max_ingest)
        self.assertTrue(retval)

    def testClusterProvisionIngest(self):
        pipeline_demand = 5
        self.env.process(self.cluster.run())
        peek = self.env.peek()
        self.env.process(self.cluster.provision_ingest_resources(
            pipeline_demand,
            self.observation)
        )
        self.env.run(until=1)
        self.assertEqual(1, self.env.now)
        # self.process(self.run_ingest(duration,pipeline_demand))
        # for task in self.cluster.running_tasks:
        #  	self.assertEqual(TaskStatus.RUNNING, task.task_status)
        self.assertEqual(5, len(self.cluster.resources['available']))
        self.assertEqual(5, len(self.cluster.tasks['running']))
        self.assertEqual(5, len(self.cluster.resources['available']))
        self.env.run(until=10)
        # AS Far as the simulation is concerned, we this occurs 'before' 10
        # seconds in simtime; however, the rest of the simulation is
        # operating AT time = 10, which is when the tasks are "Due" to
        # finish. Here we are running the tasks for "1 -timestep less" than
        # is necessary, but within the simulation time this allows us to run
        # "on time". I.e. at time = 10 (in the simulation), there _will_ be
        # 10 resources available, because it's the time period in which all
        # ingest tasks will finish. However, in real terms, these tasks
        # technically finished one timestep earlier.
        self.assertEqual(10, len(self.cluster.resources['available']))
        # self.env.run(until=20)
        # self.assertEqual(10, len(self.cluster.resources['available']))
        # self.assertEqual(20, self.env.now)

    def test_cluster_ingest_complex_pipelin(self):
        pass

    def run_ingest(self, duration, demand):
        retval = self.cluster.provision_ingest_resources(
            demand,
            duration
        )
        for task in self.cluster.tasks['running']:
            self.assertEqual(TaskStatus.SCHEDULED, task.task_status)


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
