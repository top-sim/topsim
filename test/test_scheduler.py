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

from algorithms.scheduling import FifoAlgorithm

from core.telescope import Observation, Telescope
from core.scheduler import Scheduler
from core.cluster import Cluster
from core.planner import Planner
from core.buffer import Buffer
from core.telescope import RunStatus
from algorithms.scheduling import FifoAlgorithm

from common import data as test_data

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)

OBSERVATION_CONFIG = 'test/data/config/observations.json'
BUFFER_CONFIG = 'test/data/config/buffer.json'
CLUSTER_CONFIG = "test/data/config/basic_spec-10.json"
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
		# There should be capacity
		self.assertEqual(0.0, self.env.now)
		ret = self.scheduler.check_ingest_capacity(observation, pipelines)
		self.assertTrue(ret)

		# Let's remove capacity to check it returns false
		tmp = self.cluster.available_resources
		self.cluster.available_resources = self.cluster.available_resources[:3]
		ret = self.scheduler.check_ingest_capacity(observation, pipelines)
		self.assertFalse(ret)
		self.cluster.available_resources = tmp
		self.assertEqual(10, len(self.cluster.available_resources))

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
			pipelines
		)
		observation.status = RunStatus.WAITING
		status = self.env.process(self.scheduler.allocate_ingest(
			observation,
			pipelines
		)
		)
		self.env.run(until=1)
		self.assertEqual(5, len(self.cluster.available_resources))
		# After 1 timestep, data in the HotBuffer should be 2
		self.assertEqual(498, self.buffer.hot.current_capacity)
		self.env.run(until=11)
		self.assertEqual(10, len(self.cluster.available_resources))
		self.assertEqual(5, len(self.cluster.finished_tasks))
		self.assertEqual(480, self.buffer.hot.current_capacity)


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

@unittest.skip
class TestSchedulerFIFO(unittest.TestCase):

	def setUp(self):
		self.env = simpy.Environment()
		sched_algorithm = FifoAlgorithm()
		self.planner = Planner(self.env, test_data.planning_algorithm,
							   test_data.machine_config)
		self.cluster = Cluster(self.env, CLUSTER_CONFIG)
		self.buffer = Buffer(self.env, self.cluster, BUFFER_CONFIG)
		self.observations = [
			Observation(
				'scheduler_observation',
				OBS_START_TME,
				OBS_DURATION,
				OBS_DEMAND,
				OBS_WORKFLOW,
				type='continuum',
				data_rate=5
			)
		]
		telescopemax = 36  # maximum number of antennas

		self.telescope = Telescope(
			self.env, OBSERVATION_CONFIG, self.scheduler, self.planner
		)
		self.scheduler = Scheduler(self.env, sched_algorithm, self.buffer,
								   self.cluster)

	def tearDown(self):
		pass

	def testSchedulerDecision(self):
		# algorithms.make_decision() will do something interesting only when we add a workflow plan to the
		# buffer.
		next(self.planner.run(self.observations[0]))
		#  Observation is what we are interested in with the algorithms, because the observation stores the plan;
		#  The observation object is what is stored in the buffer's 'observations_for_processing' queue.
		self.buffer.add_observation_to_waiting_workflows(self.observations[0])

		'''
		Lets start doing algorithms things!
		IT is important to note that the algorithms is only effective within the context of a simulation,
		as it is directly affected by calls to env.now; this means we need to run a mini-simulation in this
		test - which we can 'simulate' - haha - by using the enviroment and clever timeouts.
		We get an observaiton into the buffer, the algorithms makes a decision - what then?
		We use check_buffer to update the workflows in the algorithms workflow list
		This is called every time-step in the simulation, and is how we add workflow plans to the schedulers list
		'''

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
		while test_flag:
			next(self.algorithms.run())

# Now that a single workflow has been taken from the buffer and added to the list of workflows, we can schedule
#
# print(self.env.now)
# self.algorithms.process_workflows()

# process_workflows() is passing the workflow
