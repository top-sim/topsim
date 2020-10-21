import unittest
import simpy
import logging

from topsim.core.simulation import Simulation
from topsim.algorithms.scheduling import FifoAlgorithm

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)

TELESCOPE_CONFIG = 'test/data/config/observations.json'
CLUSTER_CONFIG = 'test/data/config/basic_spec-10.json'
BUFFER_CONFIG = 'test/data/config/buffer.json'
EVENT_FILE = 'test/data/output/sim.trace'


class TestSimulationConfig(unittest.TestCase):
    """
    docstring for TestSimulation"unittest.TestCase
    """

    def setUp(self):
        # tel = Telescope(env, buffer_obj, config, planner)
        self.event_file = EVENT_FILE
        self.env = simpy.Environment()
        self.planning_algorithm = 'heft'
        self.scheduling_algorithm = FifoAlgorithm()

    def tearDown(self):
        pass

    def testBasicConfig(self):
        simulation = Simulation(
            self.env,
            TELESCOPE_CONFIG,
            CLUSTER_CONFIG,
            BUFFER_CONFIG,
            self.planning_algorithm,
            self.scheduling_algorithm,
            EVENT_FILE,
            visualisation=False
        )
        self.assertTrue(36, simulation.telescope.total_arrays)


# @unittest.skip
class TestSimulationRuntime(unittest.TestCase):

    def setUp(self) -> None:
        event_file = EVENT_FILE
        env = simpy.Environment()
        planning_algorithm = 'heft'
        scheduling_algorithm = FifoAlgorithm()
        self.simulation = Simulation(
            env,
            TELESCOPE_CONFIG,
            CLUSTER_CONFIG,
            BUFFER_CONFIG,
            planning_algorithm,
            scheduling_algorithm,
            EVENT_FILE,
            visualisation=False
        )

    def testLimitedRuntime(self):
        self.simulation.start(runtime=60)


# BASIC WORKFLOW DATA
BASIC_WORKFLOW = 'test/basic-workflow-data/basic_workflow_config.json'
BASIC_CLUSTER = 'test/basic-workflow-data/basic_config.json'
BASIC_BUFFER = 'test/basic-workflow-data/basic_buffer.json'
BASIC_PLAN = 'test/basic-workflow-data/basic_observation_plan.json'


class TestSimulationBasicSetup(unittest.TestCase):

    def setUp(self) -> None:
        event_file = EVENT_FILE
        env = simpy.Environment()
        planning_algorithm = 'heft'
        scheduling_algorithm = FifoAlgorithm()
        self.simulation = Simulation(

        )
