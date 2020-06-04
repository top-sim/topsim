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
from algorithms.scheduling import FifoAlgorithm

from core.telescope import Observation, Telescope
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
		self.observations = [Observation('scheduler_observation',
									OBS_START_TME,
									OBS_DURATION,
									OBS_DEMAND,
									OBS_WORKFLOW)]
		telescopemax = 36 # maximum number of antennas
		self.telescope = Telescope(self.env, self.observations,self.buffer,telescopemax,self.planner)
		self.scheduler = Scheduler(self.env, sched_algorithm, self.buffer, self.cluster, self.telescope)

	def tearDown(self):
		pass

	"""
	The scheduler should be initialised with no workflow plans and valid pairs. 
	"""
	def testSchedulerInit(self):

		pass

	def testSchedulerDecision(self):
		# algorithms.make_decision() will do something interesting only when we add a workflow plan to the
		# buffer.
		next(self.planner.run(self.observations[0]))
		#  Observation is what we are interested in with the algorithms, because the observation stores the plan;
		#  The observation object is what is stored in the buffer's 'observations_for_processing' queue.
		self.buffer.add_observation_to_waiting_workflows(self.observations[0])

		# Lets start doing algorithms things!
		# IT is important to note that the algorithms is only effective within the context of a simulation,
		# as it is directly affected by calls to env.now; this means we need to run a mini-simulation in this
		# test - which we can 'simulate' - haha - by using the enviroment and clever timeouts.
		# We get an observaiton into the buffer, the algorithms makes a decision - what then?
		# We use check_buffer to update the workflows in the algorithms workflow list
		# This is called every time-step in the simulation, and is how we add workflow plans to the schedulers list
		test_flag = True
		self.env.process(self.scheduler.run())
		self.env.run(until=1)
		print(self.env.now)
		# We should be able to get this working nicely
		"""
		For this experiment, we are running the scheduler on a single observation, and getting it 
		to allocate a task to the required machine. the first task should be scheduled at T = 0, 
		so at t = 1, we should check to make sure that the target has been scheduled, and that it is on the appropriate 
		machine
		"""
		# Generate list of IDs
		expected_machine = "cat2_m2"
		expected_task_no = 0
		self.assertTrue(self.cluster.running_tasks)
		for m in self.cluster.machines:
			if m.id == expected_machine:
				self.assertEqual(m.current_task.id, expected_task_no)
		# Need to assert that there is something in cluster.running_tasks
		# first element of running tasks should be the first task
		self.env.run(until=100)
		print(self.env.now)
		# while test_flag:
		# 	next(self.algorithms.run())

		# Now that a single workflow has been taken from the buffer and added to the list of workflows, we can schedule
		#
		# print(self.env.now)
		# self.algorithms.process_workflows()

		# process_workflows() is passing the workflow

