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
import os

from topsim.core.config import Config
from topsim.core.planner import Planner
from topsim.core.cluster import Cluster
from topsim.core.buffer import Buffer
from topsim.core.instrument import Observation
from topsim.core.delay import DelayModel

from topsim.user.scheduling import FifoAlgorithm

current_dir = os.path.abspath('')

# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15

PLAN_ALGORITHM = 'heft'


HEFT_CLUSTER_CONFIG = "test/data/config/system_config.json"
CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"
CONFIG = "test/data/config/standard_simulation.json"
HEFT_CONFIG = "test/data/config/heft_single_observation_simulation.json"
MACHINE_CONFIG = None
OBS_WORKFLOW = "test/data/config/workflow_config.json"


class TestPlannerConfig(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(CONFIG)
        self.cluster = Cluster(env=self.env, config=config)
        self.buffer = Buffer(env=self.env, cluster=self.cluster, config=config)

    def testPlannerBasicConfig(self):
        planner = Planner(self.env, PLAN_ALGORITHM, self.cluster)
        available_resources = planner.cluster_to_shadow_format()
        self.assertEqual(1.0, available_resources['system']['bandwidth'])
        machine = available_resources['system']['resources']['cat0_m0']
        self.assertEqual(
            84, available_resources['system']['resources']['cat0_m0']['flops']
        )


class TestWorkflowPlan(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(CONFIG)
        self.cluster = Cluster(self.env, config=config)
        self.buffer = Buffer(env=self.env, cluster=self.cluster, config=config)

        self.planner = Planner(self.env, PLAN_ALGORITHM, self.cluster)
        self.observation = Observation(
            'planner_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            type=None,
            data_rate=None
        )

    def tearDown(self):
        pass

    def testWorkflowPlanCreation(self):
        time = self.env.now
        self.assertRaises(
            RuntimeError,
            next, self.planner.run(self.observation, self.buffer)
        )
        self.observation.ast = self.env.now

        plan = self.planner.plan(
            self.observation,
            self.observation.workflow,
            'heft',
            self.buffer
        )
        expected_exec_order = [0, 5, 3, 4, 2, 1, 6, 8, 7, 9]
        self.assertEqual(len(plan.tasks), len(expected_exec_order))
        for x in range(len(plan.tasks)):
            self.assertEqual(
                plan.tasks[x].id,
                'planner_observation_{0}_{1}'.format(
                    time, expected_exec_order[x]
                )
            )
        # Get taskid 5
        task5_comp = plan.tasks[5].flops
        self.assertEqual(task5_comp, 92000)


class TestPlannerDelay(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        sched_algorithm = FifoAlgorithm()
        config = Config(HEFT_CONFIG)
        dm = DelayModel(0.1, "normal")
        self.cluster = Cluster(self.env, config=config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.planner = Planner(self.env, PLAN_ALGORITHM, self.cluster, dm)
        self.observation = Observation(
            'planner_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            type=None,
            data_rate=None
        )

    def testShadowIntegration(self):
        pass

    def testPlannerRun(self):
        """
        because run() is a generator (we call yield for simpy),
        we use(next()) to 'get the return value',
        and thus run the rest of the code in run()  next(val)
        """

        self.assertRaises(
            RuntimeError,
            next,
            self.planner.run(self.observation, self.buffer)
        )
        self.observation.ast = self.env.now
        next(self.planner.run(self.observation, self.buffer))
        self.assertTrue(self.observation.plan is not None)
        self.assertTrue(0.1, self.observation.plan.tasks[0].delay.prob)

    def testGracefulExit(self):
        pass

    def testIncorrectParameters(self):
        pass
