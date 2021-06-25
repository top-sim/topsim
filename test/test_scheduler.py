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

from topsim.core.scheduler import Scheduler, ScheduleStatus
from topsim.core.cluster import Cluster
from topsim.core.planner import Planner
from topsim.core.buffer import Buffer
from topsim.core.instrument import RunStatus
from topsim.core.delay import DelayModel

from topsim.user.telescope import Telescope
from topsim.user.dynamic_plan import DynamicAlgorithmFromPlan

logging.basicConfig(level="WARNING")
logger = logging.getLogger(__name__)

CONFIG = "test/data/config_update/standard_simulation.json"
INTEGRATION = "test/data/config_update/integration_simulation.json"
HEFT_CONFIG = "test/data/config_update/heft_single_observation_simulation.json"
LONG_CONFIG = "test/data/config/mos_sw10_long.json"
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
            self.env, self.buffer, self.cluster, DynamicAlgorithmFromPlan
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


class TestSchedulerDynamicPlanAllocation(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        sched_algorithm = DynamicAlgorithmFromPlan()
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
        l = [0, 3, 2, 4, 1, 5, 6, 8, 7, 9]
        exec_ord = [
            curr_obs.name + '_' + str(self.env.now) + '_' + str(tid) for tid
            in l
        ]
        self.scheduler.observation_queue.append(curr_obs)
        curr_obs.ast = self.env.now
        self.env.process(self.planner.run(curr_obs, self.buffer))
        self.env.process(self.scheduler.allocate_tasks(curr_obs))
        self.env.run(1)
        self.assertListEqual(
            l,
            [a.task.tid for a in curr_obs.plan.exec_order]
        )
        self.buffer.cold[0].observations['stored'].append(curr_obs)
        self.env.run(until=99)
        self.assertEqual(10, len(self.cluster.tasks['finished']))
        self.assertEqual(0, len(self.cluster.tasks['running']))
        self.assertEqual(0, len(self.scheduler.observation_queue))


class TestSchedulerLongWorkflow(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        sched_algorithm = DynamicAlgorithmFromPlan()
        config = Config(LONG_CONFIG)
        self.cluster = Cluster(self.env, config)
        self.planner = Planner(self.env, PLANNING_ALGORITHM,
                               self.cluster)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.scheduler = Scheduler(self.env, self.buffer,
                                   self.cluster, sched_algorithm)
        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )

    def testAllocationTasksLongWorkflow(self):
        curr_obs = self.telescope.observations[0]
        self.scheduler.observation_queue.append(curr_obs)
        curr_obs.ast = self.env.now
        self.env.process(self.planner.run(curr_obs, self.buffer))
        self.env.process(self.scheduler.allocate_tasks(curr_obs))
        self.env.run(1)
        self.buffer.cold[0].observations['stored'].append(curr_obs)
        self.env.run(until=299)
        self.assertEqual(0, len(self.scheduler.observation_queue))


class TestSchedulerIntegration(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(INTEGRATION)
        self.cluster = Cluster(self.env, config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.planner = Planner(self.env, PLANNING_ALGORITHM,
                               self.cluster)

        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, DynamicAlgorithmFromPlan()
        )
        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        self.scheduler.start()
        self.env.process(self.scheduler.run())
        self.env.process(self.telescope.run())

    def test_FIFO_with_buffer(self):
        """
        Demonstrate that the scheduler accurately schedules when we have
        other Actors working in tandem.

        Expectations:
            - After 1 timestep in the simualtion, we have 5 resources
            available of the 10 that we start with.
            -
        Returns
        -------

        """
        self.env.run(until=1)

        self.assertEqual(10, len(self.cluster.resources['available']))
        # This takes timestep, data in the HotBuffer should be 4
        self.env.run(until=2)
        self.assertEqual(5, len(self.cluster.resources['available']))
        self.assertEqual(496, self.buffer.hot[0].current_capacity)
        self.env.run(until=31)
        self.assertEqual(5, len(self.cluster.tasks['finished']))
        # self.assertEqual(500, self.buffer.hot[0].current_capacity)
        self.assertEqual(210, self.buffer.cold[0].current_capacity)
        self.env.run(until=32)
        # Ensure the time
        self.assertEqual(ScheduleStatus.ONTIME, self.scheduler.schedule_status)
        # 30 timesteps until we finish everything + 81 timesteps to complete
        # workflow plan.
        self.env.run(until=124)
        # As we have been processing the current observation, we are also
        # ingestting the next one.
        self.assertEqual(250, self.buffer.cold[0].current_capacity)


class TestSchedulerDelayHelpers(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(INTEGRATION)
        self.cluster = Cluster(self.env, config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.planner = Planner(
            self.env, PLANNING_ALGORITHM,
            self.cluster, delay_model=DelayModel(0.3, "normal")
        )

        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, DynamicAlgorithmFromPlan()
        )
        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )
        self.env.process(self.cluster.run())
        self.env.process(self.buffer.run())
        self.scheduler.start()
        self.env.process(self.scheduler.run())
        self.env.process(self.telescope.run())

    def test_propogate_delay_returns_updated_workflow(self):
        """
        When a delay is triggered, we want to ensure that we cascade this
        down throughout the task graph to determine the global affect of the
        delay on the workflow's makespan.
        Returns
        -------

        """