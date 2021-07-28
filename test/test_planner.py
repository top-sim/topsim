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
from topsim.user.telescope import Telescope
from topsim.user.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.plan.batch_planning import BatchPlanning

current_dir = os.path.abspath('')

# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_DATA_RATE = 5
OBS_PIPELINE = 'continuum'
PLAN_ALGORITHM = 'heft'

CONFIG = "test/data/config_update/standard_simulation.json"
HEFT_CONFIG = "test/data/config_update/heft_single_observation_simulation.json"
MACHINE_CONFIG = None
OBS_WORKFLOW = "test/data/config/workflow_config_minutes.json"


class TestPlannerConfig(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(CONFIG)
        self.model = SHADOWPlanning('heft')
        self.cluster = Cluster(env=self.env, config=config)
        self.buffer = Buffer(env=self.env, cluster=self.cluster, config=config)

    def testPlannerBasicConfig(self):
        planner = Planner(
            env=self.env,
            algorithm=PLAN_ALGORITHM,
            cluster=self.cluster,
            model=self.model
        )
        available_resources = planner.model._cluster_to_shadow_format(
            self.cluster)
        # Bandwidth set at 1gb/s = 60gb/min.
        self.assertEqual(60.0, available_resources['system']['bandwidth'])
        machine = available_resources['system']['resources']['cat0_m0']
        self.assertEqual(
            5040, available_resources['system']['resources']['cat0_m0']['flops']
        )


class TestWorkflowPlan(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        config = Config(CONFIG)
        self.model = SHADOWPlanning('heft')
        self.cluster = Cluster(self.env, config=config)
        self.buffer = Buffer(env=self.env, cluster=self.cluster, config=config)

        self.planner = Planner(
            self.env, PLAN_ALGORITHM, self.cluster, self.model,
        )
        self.observation = Observation(
            'planner_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            data_rate=OBS_DATA_RATE
        )

    def tearDown(self):
        pass

    def testWorkflowPlanCreation(self):
        """
        Notes
        -------
        id rank
        0 6421.0
        5 4990.0
        3 4288.0
        4 4240.0
        2 3683.0
        1 4077.0
        6 2529.0
        8 2953.0
        7 2963.0
        9 1202.0
        Returns
        -------
        True if passes all tests, false otherwise
        """

        time = self.env.now
        # self.assertRaises(
        #     RuntimeError,
        #     next, self.planner.run(self.observation, self.buffer)
        # )
        self.observation.ast = self.env.now
        plan = self.planner.run(self.observation, self.buffer)
        # plan = self.planner.plan(
        #     self.observation,
        #     self.observation.workflow,
        #     'heft',
        #     self.buffer
        # )

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
        self.assertEqual(task5_comp, 5520000)


class TestPlannerDelay(unittest.TestCase):

    def setUp(self):
        self.env = simpy.Environment()
        sched_algorithm = DynamicAlgorithmFromPlan()
        config = Config(HEFT_CONFIG)
        dm = DelayModel(0.1, "normal")
        self.model = SHADOWPlanning('heft', dm)
        self.cluster = Cluster(self.env, config=config)
        self.buffer = Buffer(self.env, self.cluster, config)
        self.planner = Planner(self.env, PLAN_ALGORITHM, self.cluster,
                               self.model, delay_model=dm)
        self.observation = Observation(
            'planner_observation',
            OBS_START_TME,
            OBS_DURATION,
            OBS_DEMAND,
            OBS_WORKFLOW,
            data_rate=OBS_DATA_RATE
        )

    def testShadowIntegration(self):
        pass

    def testPlannerRun(self):
        """
        because run() is a generator (we call yield for simpy),
        we use(next()) to 'get the return value',
        and thus run the rest of the code in run()  next(val)
        """

        # self.assertRaises(
        #     RuntimeError,
        #     next,
        #     self.planner.run(self.observation, self.buffer)
        # )
        self.observation.ast = self.env.now
        self.observation.plan = self.planner.run(self.observation, self.buffer)
        self.assertTrue(self.observation.plan is not None)
        self.assertTrue(0.1, self.observation.plan.tasks[0].delay.prob)

    def testGracefulExit(self):
        pass

    def testIncorrectParameters(self):
        pass


class TestBatchProcessingPlan(unittest.TestCase):

    def setUp(self) -> None:
        """
        Create a planner and a `simpy` environment in which to run dummy
        simulations for the purpose of ensuring the planner works nicely
        when selecting 'batch' as a static scheduling method.
        Returns
        -------

        """
        self.env = simpy.Environment()
        config = Config(CONFIG)
        self.model = BatchPlanning('batch')

        self.cluster = Cluster(self.env, config=config)
        self.buffer = Buffer(env=self.env, cluster=self.cluster, config=config)
        self.planner = Planner(
            self.env, PLAN_ALGORITHM, self.cluster, self.model,
        )
        self.telescope = Telescope(
            self.env, config, planner=None, scheduler=None
        )

    def test_generate_topological_sort(self):
        """
        This is the main component of the batch_planning system - we just
        return a topological sort of the tasks and a list of precedence
        resources and wrap it into the 'WorkflowPlan' object.

        Returns
        -------
        plan : core.planner.WorkflowPlan
            WorkflowPlan object for the observation
        """
        obs = self.telescope.observations[0]
        plan = self.planner.run(obs, self.buffer)
        # order [0, 5, 4, 3, 2, 6, 1, 7, 8, 9]
        self.assertIsNotNone(plan)



    def tearDown(self) -> None:
        pass
