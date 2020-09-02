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

from topsim.core.planner import Planner
from topsim.core.cluster import Cluster
from topsim.core.telescope import Observation

from topsim.algorithms.scheduling import FifoAlgorithm

current_dir = os.path.abspath('')

# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15

PLAN_ALGORITHM = 'heft'
CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"

MACHINE_CONFIG = None
OBS_WORKFLOW = "test/data/config/workflow_config.json"


class TestPlannerConfig(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		self.cluster = Cluster(env=self.env, spec=CLUSTER_CONFIG)

	def testPlannerBasicConfig(self):
		planner = Planner(self.env, PLAN_ALGORITHM, self.cluster)
		available_resources = planner.cluster_to_shadow_format()
		# TODO write tests for the plan
		self.assertEqual(1.0, available_resources['system']['bandwidth'])
		machine = available_resources['system']['resources']['cat0_m0']
		self.assertEqual(
			84, available_resources['system']['resources']['cat0_m0']['flops']
		)


@unittest.skip
class TestPlanner(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		sched_algorithm = FifoAlgorithm()
		self.planner = Planner(self.env, PLAN_ALGORITHM, MACHINE_CONFIG)
		self.cluster = Cluster(self.env, CLUSTER_CONFIG)
		# self.buffer = Buffer(self.env, self.cluster)
		# self.algorithms = Scheduler(self.env,
		# sched_algorithm, self.buffer, self.cluster)
		self.observation = Observation('planner_observation',
									   OBS_START_TME,
									   OBS_DURATION,
									   OBS_DEMAND,
									   OBS_WORKFLOW)
		pass

	def tearDown(self):
		pass

	def testShadowIntegration(self):
		pass

	def testPlanReadsFromFile(self):
		# Return a "Plan" object for provided workflow/observation
		plan = self.planner.plan(self.observation.name,
								 self.observation.workflow, PLAN_ALGORITHM)
		self.assertEqual(plan.id,
						 'planner_observation')  # Expected ID for the workflow
		self.assertEqual(plan.makespan,
						 98)  # Expected makespan for the given graph

	def testPlannerRun(self):
		next(self.planner.run(self.observation))
		# because run() is a generator (we call yield for simpy), we use(next()) to 'get the return value',
		# and thus run the rest of the code in run()
		# next(val)
		self.assertTrue(self.observation.plan is not None)

	def testGracefulExit(self):
		pass

	def testIncorrectParameters(self):
		pass


class TestWorkflowPlan(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		self.cluster = Cluster(self.env, CLUSTER_CONFIG)
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
		plan = self.planner.plan(
			self.observation.name,
			self.observation.workflow,
			'heft'
		)
		expected_exec_order = [0, 5, 3, 4, 2, 1, 6, 8, 7, 9]
		self.assertEqual(len(plan.tasks), len(expected_exec_order))
		for x in range(len(plan.tasks)):
			self.assertEqual(plan.tasks[x].id, expected_exec_order[x])
		# Get taskid 5
		task5_comp = plan.tasks[5].flops
		self.assertEqual(task5_comp, 92000)
