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
from topsim.user.telescope import Telescope
from topsim.core.task import Task

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

CONFIG = "test/data/config_update/standard_simulation_longtask.json"
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
            self.assertEqual(5040, machine.cpu)
            self.assertEqual(600, machine.bandwidth)


class TestIngest(unittest.TestCase):

    def setUp(self) -> None:
        """
        Setup a cluster and a simulation environment to test ingest pipelines
        """
        self.env = simpy.Environment()
        config = Config(CONFIG)

        self.cluster = Cluster(env=self.env, config=config)
        self.telescope = Telescope(
            self.env, config, planner=None, scheduler=None
        )
        self.observation = self.telescope.observations[0]

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
        """

        Notes:

        When we run this test, we have the simulation run 'until time 10'.
        AS Far as the simulation is concerned, we this occurs 'before' 10
        seconds in simtime; however, the rest of the simulation is
        operating AT time = 10, which is when the tasks are "Due" to
        finish. Here we are running the tasks for "1 -timestep less" than
        is necessary, but within the simulation time this allows us to run
        "on time". I.e. at time = 10 (in the simulation), there _will_ be
        10 resources available, because it's the time period in which all
        ingest tasks will finish. However, in real terms, these tasks
        technically finished one timestep earlier.

        Returns
        -------

        """
        pipeline_demand = 5
        self.env.process(self.cluster.run())
        peek = self.env.peek()
        self.env.process(self.cluster.provision_ingest_resources(
            pipeline_demand,
            self.observation)
        )
        self.env.run(until=1)
        self.assertEqual(1, self.env.now)
        self.assertEqual(5, len(self.cluster._resources['available']))
        self.assertEqual(5, len(self.cluster._tasks['running']))
        self.assertEqual(5, len(self.cluster._resources['available']))
        self.env.run(until=11)
        self.assertEqual(10, len(self.cluster._resources['available']))

    def test_ingest_capacity_check(self):
        """
        Given a pipeline demand that is too great, return 'false' to ensure
        that we do not attempt to run observations.
        """

        pipeline_demand = 11
        max_ingest = 5
        self.assertFalse(
            self.cluster.check_ingest_capacity(pipeline_demand, max_ingest)
        )

    def test_ingest_demand_exceeds_cluster_capacity_runtime(self):
        """
        If we have a pipeline that requests too many ingest pipelines then we
        need to reject its observation. If - for some unknown reasons - we
        do not call this check, make sure the simulation fails at runtime.

        Returns
        -------
        """
        pipeline_demand = 11
        self.env.process(self.cluster.provision_ingest_resources(
            pipeline_demand, self.observation
        ))
        self.assertRaises(RuntimeError, self.env.run, until=1)


class TestClusterTaskAllocation(unittest.TestCase):

    def setUp(self) -> None:
        """
        TODO
        Set up tasks and cluster to attempt allocation of tasks to resources
        """
        self.env = simpy.Environment()
        config = Config(CONFIG)

        self.cluster = Cluster(env=self.env, config=config)
        self.telescope = Telescope(
            self.env, config, planner=None, scheduler=None
        )
        self.observation = self.telescope.observations[0]
        self.machine = self.cluster.machines[0]
        self.task = Task('test_0', 0, 2, self.machine, [])

    def test_allocate_task_to_cluster(self):
        self.env.process(
            self.cluster.allocate_task_to_cluster(self.task, self.machine)
        )
        self.env.run(until=1)
        self.assertEqual(self.task, self.cluster._tasks['running'][0])

    def test_duplication_allocation(self):
        ret = self.env.process(
            self.cluster.allocate_task_to_cluster(self.task, self.machine)
        )
        self.env.run(until=1)
        newtask = Task('test_2', 8, 12, self.machine, [])
        new_ret = self.env.process(
            self.cluster.allocate_task_to_cluster(newtask, self.machine)
        )
        self.assertRaises(RuntimeError, self.env.run, until=2)


        # self.assertEqual(self.task, self.cluster.tasks['running'][0])


class TestClusterBatchScheduling(unittest.TestCase):

    def setUp(self) -> None:
        """
        Batch scheduling setup
        """
        self.env = simpy.Environment()
        config = Config(CONFIG)

        self.cluster = Cluster(env=self.env, config=config)
        self.telescope = Telescope(
            self.env, config, planner=None, scheduler=None
        )
        self.observation = self.telescope.observations[0]

    def test_provision_resources(self):
        """
        Test that resource provisioning occurs and the right number of
        resources exist in the right place

        Returns
        -------

        """
        provision_size = 5
        self.cluster.provision_batch_resources(
            provision_size, self.observation
        )
        self.assertEqual(5, len(self.cluster.get_available_resources()))
        self.assertEqual(
            5, len(self.cluster.get_idle_resources(self.observation))
        )
        self.assertListEqual(
            self.cluster._get_batch_observations(), [self.observation]
        )

    def test_batch_removal(self):
        provision_size = 5
        self.cluster.provision_batch_resources(
            provision_size, self.observation
        )
        self.cluster.release_batch_resources(self.observation)
        self.assertListEqual(
            [], self.cluster.get_idle_resources(self.observation)
        )
        self.assertEqual(10, len(self.cluster.get_available_resources()))


    def tearDown(self) -> None:
        """
        TODO
        Batch scheduling tear down
        """
        pass
