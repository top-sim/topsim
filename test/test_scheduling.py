# Copyright (C) 22/3/21 RW Bunney

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
Test the scheduling algorithm as 'designed' by the user.
"""
import simpy
import unittest

from topsim.core.config import Config
from topsim.core.planner import Planner
from topsim.core.cluster import Cluster
from topsim.core.scheduler import Scheduler
from topsim.core.buffer import Buffer
from topsim.user.telescope import Telescope

from topsim.user.plan.batch_planning import BatchPlanning
from topsim.user.schedule.batch_allocation import BatchProcessing

from topsim.user.schedule.dynamic_plan import DynamicAlgorithmFromPlan

CONFIG = "test/data/config/standard_simulation.json"


class TestFifoAlgorithm(unittest.TestCase):

    def setUp(self):
        self.algorithm = DynamicAlgorithmFromPlan()
        # TODO setup a workflow
        self.workflow = None

    def testAccurateReturn(self):
        pass

    def testTemporaryResourcesAllocation(self):
        pass

    def testRemovalofESTCheck(self):
        pass


class TestBatchSchedulerAllocation(unittest.TestCase):

    def setUp(self):
        self.algorithm = BatchProcessing
        self.env = simpy.Environment()
        config = Config(CONFIG)
        self.cluster = Cluster(self.env, config=config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.scheduler = Scheduler(
            self.env, self.buffer, self.cluster, DynamicAlgorithmFromPlan()
        )
        self.algorithm = BatchProcessing()
        self.model = BatchPlanning('batch')
        self.planner = Planner(
            self.env, 'heft', self.cluster, self.model,
        )

        self.telescope = Telescope(
            self.env, config, self.planner, self.scheduler
        )

    def test_resource_provision(self):
        """
        Given a max_resource_split of 2, and total machines of 10, we should
        provision a maximum of 5 machines within the cluster (
        max_resource_split being the number of parallel provision ings we can
        make).

        Returns
        -------

        """
        self.assertEqual(10, len(self.cluster.get_available_resources()))

    def test_max_resource_provision(self):
        obs = self.telescope.observations[0]
        self.env.process(
            self.cluster.provision_ingest_resources(5, obs)
        )
        self.env.run(until=1)
        self.assertEqual(5, len(self.cluster.get_available_resources()))
        self.assertEqual(5, self.algorithm._max_resource_provision(
            self.cluster))
        # TODO The algorithm must provision resources if they are not already
        #  provisioned.
        plan = self.planner.run(obs, self.buffer, self.telescope.max_ingest)
        self.algorithm._provision_resources(self.cluster, plan)
        self.assertEqual(5, len(self.cluster.get_idle_resources(obs.name)))
        self.assertEqual(
                0, self.algorithm._max_resource_provision(self.cluster)
        )
        # self.planner.run(obs, )

    def test_algorithm_allocation(self):
        obs = self.telescope.observations[0]
        obs.plan = self.planner.run(obs, self.buffer, self.telescope.max_ingest)
        # Replicate the Scheduler allocate_task() methods
        existing_schedule = {}
        existing_schedule, status = self.algorithm.run(
            self.cluster, self.env.now, obs.plan, existing_schedule
        )
        self.assertTrue(obs.plan.tasks[0] in existing_schedule)

    def test_observation_queue(self):
        """
        Observation queue will static once the resources are exausted
        Returns
        -------

        """
