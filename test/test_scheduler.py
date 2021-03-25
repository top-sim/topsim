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

"""
    TestSchedulerIngest:
    Uses basic_simulation.json
        Cluster is 10 machines, each with 84 flops and 10 data rate
        2 observations, both with a workflow_config.json

    TestSchedulerFIFO:
        This uses the 'classic' HEFT workflow, and a HEFT system config
        Cluster is only 3 machines

    TestIntegration:
        This uses a modified workflow_config.
"""

import unittest
import simpy
import logging

from topsim.core.config import Config
from topsim.user.telescope import Telescope
from topsim.core.scheduler import Scheduler
from topsim.core.cluster import Cluster
from topsim.core.planner import Planner
from topsim.core.buffer import Buffer
from topsim.core.instrument import RunStatus

from topsim.user.scheduling import FifoAlgorithm

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

CONFIG = "test/data/config/standard_simulation.json"
INTEGRATION = "test/data/config/integration_simulation.json"
HEFT_CONFIG = "test/data/config/heft_single_observation_simulation.json"
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
        config = Config(CONFIG)
        self.cluster = Cluster(self.env, config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, FifoAlgorithm
        )
        self.planner = Planner(self.env, PLANNING_ALGORITHM,
                               self.cluster)
        planner = None
        self.telescope = Telescope(self.env, config, planner, self.scheduler)

    def testSchdulerCheckIngestReady(self):
        """
        Check the return status of check_ingest_capacity is correct
        """
        pipelines = self.telescope.pipelines
        observation = self.telescope.observations[0]

        max_ingest = self.telescope.max_ingest
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
        pipelines = self.telescope.pipelines
        max_ingest = self.telescope.max_ingest
        observation = self.telescope.observations[0]

        ready_status = self.scheduler.check_ingest_capacity(
            observation,
            pipelines,
            max_ingest
        )
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        observation.status = RunStatus.WAITING
        status = self.env.process(
            self.scheduler.allocate_ingest(
                observation,
                pipelines,
                self.planner
            )
        )

        self.env.run(until=1)
        self.assertEqual(5, len(self.cluster.resources['available']))
        # After 1 timestep, data in the HotBuffer should be 2
        self.assertEqual(496, self.buffer.hot[0].current_capacity)
        self.env.run(until=30)
        self.assertEqual(10, len(self.cluster.resources['available']))
        self.assertEqual(5, len(self.cluster.tasks['finished']))
        self.assertEqual(500, self.buffer.hot[0].current_capacity)
        self.assertEqual(210, self.buffer.cold[0].current_capacity)


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
        config = Config(HEFT_CONFIG)
        self.cluster = Cluster(self.env, config)
        self.planner = Planner(self.env, PLANNING_ALGORITHM,
                               self.cluster)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.scheduler = Scheduler(self.env, self.buffer,
                                   self.cluster, sched_algorithm)
        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )

    def tearDown(self):
        pass

    def testAllocationTasksNoObservation(self):
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

        The allocations for the HEFT algorithm are (in sorted order):
            id - mid    - (ast,aft)
            0 - cat2_m2 - (0,11)
            3 - cat2_m2 - (11,21)
            2 - cat2_m2 - (21,30)
            4 - cat1_m1 - (22, 40)
            1 - cat0_m0 - (29,42)
            5 - cat2_m2 - (30,45)
            6 - cat2_m2 - (45, 55)
            8 - cat2_m2 - (58, 71)
            7 - cat0_m0 - (60, 61)
            9 - cat0_m0 - (84,98)

        """
        curr_obs = self.telescope.observations[0]
        gen = self.scheduler.allocate_tasks(curr_obs)
        self.assertRaises(RuntimeError, next, gen)
        l = [0, 3, 2, 4, 1,5, 6, 8, 7, 9]
        exec_ord = [
            curr_obs.name + '_' + str(self.env.now) + '_' + str(tid) for tid
            in l
        ]
        self.scheduler.observation_queue.append(curr_obs)
        curr_obs.ast = self.env.now
        self.env.process(self.planner.run(curr_obs,self.buffer))
        self.env.process(self.scheduler.allocate_tasks(curr_obs))
        self.env.run(1)
        self.assertListEqual(
            l,
            [a.task.tid for a in curr_obs.plan.exec_order]
        )
        self.buffer.cold[0].observations['stored'].append(curr_obs)
        self.env.run(until=98)
        self.assertEqual(10, len(self.cluster.tasks['finished']))
        self.assertEqual(0, len(self.cluster.tasks['running']))
        self.assertEqual(0, len(self.scheduler.observation_queue))

    def testAllocateTasksWithPreceedingObservation(self):
        pipelines = self.telescope.pipelines
        max_ingest = 5

        observation = self.telescope.observations[0]
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        self.env.process(self.telescope.run())
        self.scheduler.start()
        self.env.process(self.scheduler.run())
        # status = self.env.process(self.scheduler.allocate_ingest(
        #     observation,
        #     pipelines,
        #     self.planner
        # ))

        self.env.run(until=1)
        self.env.run(until=11)
        self.env.run(until=118)
        self.env.run(until=119)




class TestSchedulerIntegration(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(INTEGRATION)
        self.cluster = Cluster(self.env, config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.planner = Planner(self.env, PLANNING_ALGORITHM,
                               self.cluster)

        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, FifoAlgorithm()
        )
        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )

    def test_FIFO_with_buffer(self):

        pipelines = self.telescope.pipelines
        max_ingest = 5
        observation = self.telescope.observations[0]

        ready_status = self.scheduler.check_ingest_capacity(
            observation,
            pipelines,
            max_ingest
        )
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        self.scheduler.start()
        self.env.process(self.scheduler.run())

        observation.status = RunStatus.WAITING
        status = self.env.process(self.scheduler.allocate_ingest(
            observation,
            pipelines,
            self.planner
        ))
        # self.env.process(self.planner.run(observation,self.buffer))

        self.env.run(until=1)
        
        self.assertEqual(5, len(self.cluster.resources['available']))
        # After 1 timestep, data in the HotBuffer should be 2
        self.assertEqual(496, self.buffer.hot[0].current_capacity)
        self.env.run(until=30)
        self.assertEqual(5, len(self.cluster.tasks['finished']))
        self.assertEqual(500, self.buffer.hot[0].current_capacity)
        self.assertEqual(210, self.buffer.cold[0].current_capacity)
        self.env.run(until=131)

        self.assertEqual(250, self.buffer.cold[0].current_capacity)
