import unittest
import simpy
import logging
import os

from topsim.core.config import Config
from topsim.core.simulation import Simulation
from topsim.user.scheduling import GreedyAlgorithmFromPlan
from topsim.user.telescope import Telescope
logging.basicConfig(level='WARNING')
logger = logging.getLogger(__name__)

CONFIG = 'test/data/config_update/standard_simulation.json'

class TestSimulationConfig(unittest.TestCase):
    """
    docstring for TestSimulation"unittest.TestCase
    """

    def setUp(self):
        # tel = Telescope(env, buffer_obj, config, planner)
        self.env = simpy.Environment()
        self.instrument = Telescope
        self.timestamp = f'test/data/output/{0}'

    def testBasicConfig(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning='heft',
            scheduling=GreedyAlgorithmFromPlan,
            timestamp=self.timestamp
        )
        self.assertTrue(36, simulation.instrument.total_arrays)


# @unittest.skip
class TestSimulationRuntime(unittest.TestCase):

    def setUp(self) -> None:
        env = simpy.Environment()
        self.simulation = Simulation(
            env,
            CONFIG,
            Telescope,
            planning='heft',
            scheduling=GreedyAlgorithmFromPlan,
            delay=None,
            timestamp=f'test/data/output/{0}'
        )

    def tearDown(self):
        output = 'test/data/output/0-heft-GreedyAlgorithmFromPlan'
        os.remove(f'{output}-sim.pkl')
        os.remove(f'{output}-tasks.pkl')


    def testLimitedRuntime(self):
        self.simulation.start(runtime=60)


# BASIC WORKFLOW DATA

BASIC_CONFIG = 'test/basic-workflow-data/basic_simulation.json'


class TestSimulationBasicSetup(unittest.TestCase):

    def setUp(self) -> None:
        env = simpy.Environment()
        self.simulation = Simulation(
            env,
            BASIC_CONFIG,
            Telescope,
            planning='heft',
            scheduling=GreedyAlgorithmFromPlan,
            delay=None,
            timestamp=f'test/basic-workflow-data/{0}'
        )

    def tearDown(self):
        output = 'test/data/output/0-heft-GreedyAlgorithmFromPlan'
        os.remove(f'{output}-sim.pkl')
        os.remove(f'{output}-tasks.pkl')

