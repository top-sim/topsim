import unittest
import simpy
import logging

from topsim.core.config import Config
from topsim.core.simulation import Simulation
from topsim.algorithms.scheduling import FifoAlgorithm

logging.basicConfig(level='WARNING')
logger = logging.getLogger(__name__)

TELESCOPE_CONFIG = 'test/data/config/observations.json'
CLUSTER_CONFIG = 'test/data/config/basic_spec-10.json'
BUFFER_CONFIG = 'test/data/config/buffer.json'
EVENT_FILE = 'test/data/output/sim.trace'
CONFIG = 'test/data/config/standard_simulation.json'


class TestSimulationConfig(unittest.TestCase):
    """
    docstring for TestSimulation"unittest.TestCase
    """

    def setUp(self):
        # tel = Telescope(env, buffer_obj, config, planner)
        self.event_file = EVENT_FILE
        self.env = simpy.Environment()
        self.algorithm_map = {'heft': 'heft', 'fifo': FifoAlgorithm}

    def tearDown(self):
        pass

    def testBasicConfig(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            self.algorithm_map,
            self.event_file
        )
        self.assertTrue(36, simulation.telescope.total_arrays)


# @unittest.skip
class TestSimulationRuntime(unittest.TestCase):

    def setUp(self) -> None:
        event_file = EVENT_FILE
        env = simpy.Environment()
        algorithm_map = {'heft': 'heft', 'fifo': FifoAlgorithm}
        self.simulation = Simulation(
            env,
            CONFIG,
            algorithm_map,
            event_file
        )

    def testLimitedRuntime(self):
        self.simulation.start(runtime=60)


# BASIC WORKFLOW DATA
BASIC_WORKFLOW = 'test/basic-workflow-data/basic_workflow_config.json'
BASIC_CLUSTER = 'test/basic-workflow-data/basic_config.json'
BASIC_BUFFER = 'test/basic-workflow-data/basic_buffer.json'
BASIC_PLAN = 'test/basic-workflow-data/basic_observation_plan.json'

BASIC_CONFIG = 'test/basic-workflow-data/basic_simulation.json'


class TestSimulationBasicSetup(unittest.TestCase):

    def setUp(self) -> None:
        env = simpy.Environment()
        algorithm_map = {'heft': 'heft', 'fifo': FifoAlgorithm}
        self.simulation = Simulation(
            env,
            BASIC_CONFIG,
            algorithm_map,
            EVENT_FILE,
        )
