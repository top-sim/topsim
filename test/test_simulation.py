import unittest
import simpy

from core.simulation import Simulation
from algorithms.scheduling import FifoAlgorithm
from common import data as test_data
import logging

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


@unittest.skip
class TestSimulation(unittest.TestCase):
	"""docstring for TestSimulation"unittest.TestCase"""

	def setUp(self):
		env = None
		# tel = Telescope(env, buffer_obj, config, planner)
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
