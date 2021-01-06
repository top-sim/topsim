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

from topsim.common import data as test_data
from topsim.core.config import Config
from topsim.core.planner import Planner
from topsim.core.telescope import Observation, RunStatus
from topsim.core.buffer import Buffer
from topsim.core.cluster import Cluster

# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_WORKFLOW = 'test/data/config/workflow_config.json'
PLAN_ALGORITHM = 'heft'

CONFIG = "test/data/config/standard_simulation.json"


class TestBufferConfig(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        self.config = Config(CONFIG)
        self.cluster = Cluster(env=self.env, config=self.config)

    def testHotBufferConfig(self):
        """
        Process the Hot Buffer section of the config file
        """
        buffer = Buffer(
            env=self.env, cluster=self.cluster, config=self.config
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
            env=self.env, cluster=self.cluster, config=self.config
        )
        self.assertEqual(250, buffer.cold.total_capacity)
        self.assertEqual(250, buffer.cold.current_capacity)
        self.assertEqual(2, buffer.cold.max_data_rate)


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
        self.config = Config(CONFIG)
        self.cluster = Cluster(env=self.env, config=self.config)
        self.buffer = Buffer(self.env, self.cluster, self.config)
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

        We test a couple of requirements here.
        """
        self.observation.status = RunStatus.RUNNING

        ret = self.env.process(
            self.buffer.ingest_data_stream(
                self.observation
            )
        )
        self.env.run(until=1)
        self.assertEqual(495, self.buffer.hot.current_capacity)
        self.env.run(until=10)
        self.assertEqual(self.env.now, 10)
        self.assertEqual(450, self.buffer.hot.current_capacity)
        self.assertEqual(
            self.buffer.hot.observations["stored"][0],
            self.observation
        )

    def testIngestPrerequisites(self):
        """
        In order to call ingest_data_stream, the observation must be marked as
        "RunStatus.RUNNING" - otherwise we will  be processing an observation
        that hasn't started!

        -------

        """
        ret = self.env.process(
            self.buffer.ingest_data_stream(
                self.observation
            )
        )

        self.assertRaises(
            RuntimeError, self.env.run, until=1
        )

    def testIngestObservationNotRunning(self):
        """
        The buffer won't ingest if the observation is not marked as
        RunStatus.RUNNING
        """

        self.assertEqual(RunStatus.WAITING, self.observation.status)
        self.env.process(self.buffer.ingest_data_stream(self.observation))
        # self.assertRaises(
        # 	RuntimeError, self.env.process, self.buffer.ingest_data_stream(
        # 		self.observation
        # 	)
        # )

        self.assertRaises(
            RuntimeError, self.env.run, until=1
        )

    # self.assertEqual(500, self.buffer.hot.current_capacity)

    def testIngestEdgeCase(self):
        """
        Buffer must accept ingest at rate up to 'max ingest data rate' but
        raises an exception if the ingest rate for an observation is greater
        (this means we have an error).

        In addition, we are coordinating this ingest between the scheduler and
        the telescope and the cluster so these actors also need to work
        together in some way, which this test will also attempt to do .

        """
        self.observation.status = RunStatus.RUNNING

        self.observation.ingest_data_rate = 20
        ret = self.env.process(
            self.buffer.ingest_data_stream(
                self.observation
            )
        )
        self.assertRaises(ValueError, self.env.run, until=1)

    def test_ingest_capacity_checks(self):
        """

        The buffer checks the hot and cold buffer for capacity;
        need to make sure that if either the hot buffer or the cold buffer do
        not have enough room for the observation, it is not scheduled.

        Returns
        -------

        """

        self.buffer.hot.current_capacity = 2
        self.assertFalse(
            self.buffer.check_buffer_capacity(self.observation)
        )

        self.buffer.hot.current_capacity = 100
        self.buffer.cold.current_capacity = 1
        self.assertFalse(
            self.buffer.check_buffer_capacity(self.observation)
        )


class TestBufferRequests(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        self.config = Config(CONFIG)
        self.cluster = Cluster(env=self.env, config=self.config)

        self.buffer = Buffer(
            env=self.env, cluster=self.cluster, config=self.config
        )
        self.planner = Planner(self.env, PLAN_ALGORITHM, self.cluster)
        self.observation = Observation(
            'scheduler_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            type='continuum',
            data_rate=2
        )

    def tearDown(self):
        pass

    def test_buffer_hot_to_cold(self):
        """
        This tests an ingest, and then, once we have a successful ingest,
        the movement from one buffer to the other.

        Using the current situation, we should have the observation finished by
        timestep [TBC], and then the observation moved across by timestep [TBC]

        Returns
        -------

        """
        self.observation.status = RunStatus.RUNNING
        self.env.process(self.buffer.ingest_data_stream(self.observation))
        self.env.run(until=10)
        self.assertEqual(480, self.buffer.hot.current_capacity)

        # Moving data from one to the other
        self.assertEqual(250, self.buffer.cold.current_capacity)
        self.assertTrue(self.observation in
                         self.buffer.hot.observations["stored"])
        self.env.process(self.buffer.move_hot_to_cold())
        self.env.run(until=15)
        self.assertEqual(240, self.buffer.cold.current_capacity)
        self.assertEqual(490, self.buffer.hot.current_capacity)
        self.env.run(until=20)
        self.assertEqual(230, self.buffer.cold.current_capacity)
        self.env.run(until=22)
        self.assertEqual(500, self.buffer.hot.current_capacity)
        self.assertEqual(230, self.buffer.cold.current_capacity)
        self.assertListEqual(
            [self.observation], self.buffer.cold.observations['stored']
        )

    def test_hot_transfer_observation(self):
        """
        When passed an observation, over a period of time ensure that the
        complete data set is removed.

        Only when all data has finished being transferred do we add the
        observation to ColdBuffer.observations.

        Observation duration is 10; ingest rate is 5.

        Observation.total_data_size => 50

        ColdBuffer.max_data_rate => 2; therefore

        Time until Observation is moved => 25.

        Returns
        -------
        """
        self.buffer.hot.current_capacity = 450
        self.observation.total_data_size = 50
        data_left_to_transfer = self.observation.total_data_size
        self.buffer.hot.observations["stored"].append(self.observation)
        data_left_to_transfer = self.buffer.hot.transfer_observation(
            self.observation,
            self.buffer.cold.max_data_rate,
            data_left_to_transfer
        )
        self.assertEqual(48, data_left_to_transfer)
        self.assertTrue(
            self.observation in self.buffer.hot.observations["stored"]
        )
        self.assertEqual(452, self.buffer.hot.current_capacity)
        timestep = 24
        while data_left_to_transfer > 0:
            data_left_to_transfer = self.buffer.hot.transfer_observation(
                self.observation,
                self.buffer.cold.max_data_rate,
                data_left_to_transfer
            )
        self.assertEqual(0, data_left_to_transfer)
        self.assertEqual(500, self.buffer.hot.current_capacity)

    def test_cold_receive_data(self):
        """
        When passed an observation, over a period of time ensure that the
        complete data set is added to the Cold Buffer.

        Only when all data has finished being transferred do we add the
        observation to ColdBuffer.observations.

        Observation duration is 10; ingest rate is 5.

        Observation.total_data_size => 50

        ColdBuffer.max_data_rate => 2; therefore

        Time until Observation is moved => 25.

        Returns
        -------
        """
        self.observation.total_data_size = 50
        data_left_to_transfer = self.observation.total_data_size
        data_left_to_transfer = self.buffer.cold.receive_observation(
            self.observation,
            data_left_to_transfer
        )
        self.assertEqual(48, data_left_to_transfer)
        self.assertFalse(
            self.observation in self.buffer.cold.observations['stored']
        )

        while data_left_to_transfer > 0:
            data_left_to_transfer = self.buffer.cold.receive_observation(
                self.observation,
                data_left_to_transfer
            )
        self.assertTrue(
            self.observation in self.buffer.cold.observations['stored']
        )
        self.assertEqual(None, self.buffer.cold.observations['transfer'])

    # @unittest.skip("Functionality has changed")
    def testWorkflowAddedToQueue(self):
        """
        We only add a workflow to the queue once an observation has finished
        (and, therefore, after we have finished generating a plan for it).
        :return: None
        """

        # Calling planner.run() will store the generate plan in the observation object
        # calling next() runs the iterator immediately after generator is called
        next(self.planner.run(self.observation))
        self.assertTrue(self.observation.plan is not None)
        # Buffer observation queue should be empty
#
# # Get the observation and check we have applied the buffer offset
# self.assertTrue(self.observation.start > OBS_START_TME + OBS_DURATION)
