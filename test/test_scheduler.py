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

from core.simulation import Simulation
from scheduler.fifo_algorithm import FifoAlgorithm

from core.telescope import Observation
from core.scheduler import Scheduler
from core.cluster import Cluster
from core.planner import Planner, WorkflowPlan
from core.buffer import Buffer
from core.simulation import _process_telescope_config

import test_data


# Globals
OBS_START_TME = 0
OBS_DURATION = 10
OBS_DEMAND = 15
OBS_WORKFLOW = test_data.test_scheduler_workflow

class TestSchedulerRandom(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass


class TestSchedulerFIFO(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		sched_algorithm = FifoAlgorithm()
		self.planner = Planner(self.env, test_data.planning_algorithm, test_data.machine_config)
		self.cluster = Cluster(test_data.machine_config)
		self.buffer = Buffer(self.env, self.cluster)
		self.scheduler = Scheduler(self.env, sched_algorithm, self.buffer, self.cluster)
		self.observation = Observation('scheduler_observation',
									OBS_START_TME,
									OBS_DURATION,
									OBS_DEMAND,
									OBS_WORKFLOW)

	def tearDown(self):
		pass

	"""
	The scheduler should be initialised with no workflow plans and valid pairs. 
	"""
	def testSchedulerInit(self):

		pass

	def testSchedulerDecision(self):
		# scheduler.make_decision() will do something interesting only when we add a workflow plan to the
		# buffer.
		next(self.planner.run(self.observation))
		#  Observation is what we are interested in with the scheduler, because the observation stores the plan;
		#  The observation object is what is stored in the buffer's 'observations_for_processing' queue.
		self.buffer.add_observation_to_waiting_workflows(self.observation)

		# Lets start doing scheduler things!
		# We get an observaiton into the buffer, the scheduler makes a decision - what then?


		pass
