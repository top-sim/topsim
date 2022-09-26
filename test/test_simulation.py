import unittest
import simpy
import logging
import os
import datetime
import pandas as pd
from pathlib import Path

from topsim.core.simulation import Simulation
from topsim.user.schedule.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.plan.batch_planning import BatchPlanning
from topsim.user.telescope import Telescope

logging.basicConfig(level='WARNING')
logger = logging.getLogger(__name__)

cwd = os.getcwd()
CONFIG = Path(f'{cwd}/test/data/config/standard_simulation.json')


class TestSimulationConfig(unittest.TestCase):
    """
    docstring for TestSimulation"unittest.TestCase
    """

    def setUp(self):
        # tel = Telescope(env, buffer_obj, config, planner)
        self.env = simpy.Environment()
        self.instrument = Telescope
        self.timestamp = 0

    def testBasicConfig(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning_model=SHADOWPlanning('heft'),
            planning_algorithm='heft',
            scheduling=DynamicAlgorithmFromPlan(),
            timestamp=self.timestamp
        )
        self.assertTrue(36, simulation.instrument.total_arrays)


# @unittest.skip
class TestSimulationFileOptions(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        self.output = f'test/data/output/hdf5.h5'

    def tearDown(self):
        output = f'test/data/output/hdf5.h5'
        os.remove(output)
        # os.remove(f'{output}')
        # os.remove(f'{output}-tasks.pkl')

    def test_simulation_produces_file(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            Telescope,
            planning_model=SHADOWPlanning('heft'),
            planning_algorithm='heft',
            scheduling=DynamicAlgorithmFromPlan(),
            delay=None,
            timestamp=0,
            to_file=True,
            hdf5_path=self.output
        )

        simulation.start(runtime=60)
        self.assertTrue(os.path.exists(self.output))

        store = pd.HDFStore(self.output)
        store.close()

        # store[f'{s}/standard_simulation/sim']

    # def


class TestSimulationBatchProcessing(unittest.TestCase):
    def setUp(self) -> None:
        self.env = simpy.Environment()

    def test_run_batchmodel(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            Telescope,
            planning_model=BatchPlanning('batch'),
            planning_algorithm='batch',
            scheduling=BatchProcessing(),
            delay=None,
            timestamp=0
        )
        sim, task = simulation.start()
        self.assertGreater(len(sim), 0)
