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
import logging

from topsim.core.telescope import Observation, Telescope
from topsim.core.scheduler import Scheduler
from topsim.core.cluster import Cluster
from topsim.core.planner import Planner
from topsim.core.buffer import Buffer
from topsim.core.telescope import RunStatus
from topsim.algorithms.scheduling import FifoAlgorithm

from topsim.common import data as test_data

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)

OBSERVATION_CONFIG = 'test/data/config/observations.json'
BUFFER_CONFIG = 'test/data/config/buffer.json'
CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"

HEFT_CLUSTER_CONFIG = 'test/data/config/system_config.json'
HEFT_WORKFLOW = 'test/data/config/workflow_config.json'
# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_WORKFLOW = "test/data/config/workflow_config.json"
PLANNING_ALGORITHM = 'heft'


class TestSchedulerRandom(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


class TestSchedulerIngest(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        self.cluster = Cluster(self.env, CLUSTER_CONFIG)
        self.buffer = Buffer(self.env, self.cluster, BUFFER_CONFIG)
        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, FifoAlgorithm
        )

    def testSchdulerCheckIngestReady(self):
        """
        Check the return status of check_ingest_capacity is correct
        """
        pipelines = {
            "continuum": {
                "demand": 5
            }
        }
        observation = Observation(
            'planner_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            type="continuum",
            data_rate=2
        )

        max_ingest = 5

        # There should be capacity
        self.assertEqual(0.0, self.env.now)
        ret = self.scheduler.check_ingest_capacity(
            observation, pipelines, max_ingest
        )
        self.assertTrue(ret)

        # Let's remove capacity to check it returns false
        tmp = self.cluster.resources['available']
        self.cluster.resources['available'] = self.cluster.resources[
                                                  'available'][:3]
        ret = self.scheduler.check_ingest_capacity(
            observation, pipelines, max_ingest
        )
        self.assertFalse(ret)
        self.cluster.resources['available'] = tmp
        self.assertEqual(10, len(self.cluster.resources['available']))

    def testSchedulerProvisionsIngest(self):
        """
        Ensure that the scheduler correcly coordinates ingest onto the Cluster
        and into the Buffer

        Returns
        -------
        """
        pipelines = {
            "continuum": {
                "demand": 5
            }
        }
        max_ingest = 5
        observation = Observation(
            'planner_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            type="continuum",
            data_rate=2
        )

        ready_status = self.scheduler.check_ingest_capacity(
            observation,
            pipelines,
            max_ingest
        )
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        observation.status = RunStatus.WAITING
        status = self.env.process(self.scheduler.allocate_ingest(
            observation,
            pipelines
        )
        )

        self.env.run(until=1)
        self.assertEqual(5, len(self.cluster.resources['available']))
        # After 1 timestep, data in the HotBuffer should be 2
        self.assertEqual(498, self.buffer.hot.current_capacity)
        self.env.run(until=20)
        self.assertEqual(10, len(self.cluster.resources['available']))
        self.assertEqual(5, len(self.cluster.tasks['finished']))
        self.assertEqual(500, self.buffer.hot.current_capacity)
        self.assertEqual(230, self.buffer.cold.current_capacity)


"""
TODO 
Global DAG internalisation needs work (unimplemented as it currently stands)
Maybe this is implicit? I.e. we visualise it but there is no explicit 
concept of it in the function of the simulation? 

Scheduler post-ingest calculations and start-times that we need to iron out

* Movement from hot-buffer to cold buffer
* Data 'provisioning' for initial workflow node
* Machine 'provisioning' for the workflow based on Cluster and GLOBAL DAG
"""


class TestSchedulerFIFO(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        sched_algorithm = FifoAlgorithm()
        self.cluster = Cluster(self.env, HEFT_CLUSTER_CONFIG)
        self.planner = Planner(self.env, PLANNING_ALGORITHM,
                               self.cluster)
        self.buffer = Buffer(self.env, self.cluster, BUFFER_CONFIG)
        self.observations = [
            Observation(
                'scheduler_observation',
                OBS_START_TME,
                OBS_DURATION,
                OBS_DEMAND,
                OBS_WORKFLOW,
                type='continuum',
                data_rate=2
            )
        ]
        self.scheduler = Scheduler(self.env, self.buffer,
                                   self.cluster, sched_algorithm)
        self.telescope = Telescope(
            self.env, OBSERVATION_CONFIG, self.scheduler, self.planner
        )

    def tearDown(self):
        pass

    def test_allocate_tasks(self):
        """
        allocate_tasks assumes we have:

            * An observation stored in the ColdBuffer
            * A plan stored for that observation
            * Access to a scheduling algorithm (in this case, FifoAlgorithm).

        Need to check:
            * If there is no current observation, we can't do anthing
            * If there is an observation, but no plan, we assign the
            observation planto the current_plan.
            * Once things are running, we make sure things are being
            scheduled onto the right machines
            * They should also be running for the correct period of time.
        """

        self.assertFalse(self.scheduler.allocate_tasks())

        curr_obs = self.observations[0]
        self.scheduler.current_observation = curr_obs
        self.assertRaises(RuntimeError, self.scheduler.allocate_tasks)
        self.env.process(self.planner.run(self.scheduler.current_observation))
        self.env.run(1)
        self.scheduler.allocate_tasks(test=True)
        self.assertListEqual(
            [0, 2, 3, 1, 5, 4, 6, 8, 7, 9],
            [a.task.tid for a in self.scheduler.current_plan.exec_order]
        )
        # self.scheduler.init()
        self.buffer.cold.observations['stored'].append(curr_obs)
        self.scheduler.allocate_tasks()
        self.env.run(until=5)
        self.assertEqual(1,len(self.cluster.tasks['running']))


