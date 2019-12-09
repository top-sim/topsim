import unittest
import simpy

from core.simulation import Simulation
from core.telescope import Telescope
from scheduler.fifo_algorithm import FifoAlgorithm
import test_data


class TestSimulation(unittest.TestCase):
	"""docstring for TestSimulation"unittest.TestCase"""

	def setUp(self):
		env = None
		# tel = Telescope(env, observations, buffer_obj, telescope_config, planner)
		event_file = test_data.event_file
		planning_algorithm = test_data.planning_algorithm
		env = simpy.Environment()
		tmax = 36  # for starters, we will define telescope configuration as simply number of arrays that exist
		sched_algorithm = FifoAlgorithm()
		self.simulation = Simulation(env, test_data.telescope_config, tmax, test_data.machine_config,
									 sched_algorithm, planning_algorithm, event_file)

	def tearDown(self):
		pass

	def testBasicRun(self):
		self.simulation.start(runtime=60)