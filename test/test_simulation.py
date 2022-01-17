import unittest
import simpy
import logging
import os
import datetime
import pandas as pd

from topsim.core.simulation import Simulation
from topsim.user.schedule.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.plan.batch_planning import BatchPlanning
from topsim.user.telescope import Telescope

logging.basicConfig(level='WARNING')
logger = logging.getLogger(__name__)

cwd = os.getcwd()
CONFIG = f'{cwd}/test/data/config/standard_simulation.json'


class TestSimulationConfig(unittest.TestCase):
    """
    docstring for TestSimulation"unittest.TestCase
    """

    def setUp(self):
        # tel = Telescope(env, buffer_obj, config, planner)
        self.env = simpy.Environment()
        self.instrument = Telescope
        self.timestamp = f'{cwd}/test/data/output/{0}'

    def testBasicConfig(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning_model=SHADOWPlanning('heft'),
            planning_algorithm='heft',
            scheduling=DynamicAlgorithmFromPlan,
            timestamp=self.timestamp
        )
        self.assertTrue(36, simulation.instrument.total_arrays)


# @unittest.skip
class TestSimulationFileOptions(unittest.TestCase):

    def setUp(self) -> None:
        self.env = simpy.Environment()
        self.output = f'{cwd}/test/data/output/hdf5.h5'

    def tearDown(self):
        output = f'{cwd}/test/data/output/hdf5.h5'
        # os.remove(f'{output}')
        # os.remove(f'{output}-tasks.pkl')

    def test_simulation_produces_file(self):
        ts = f'{datetime.datetime(2021,1,1).strftime("%y_%m_%d_%H_%M_%S")}'
        simulation = Simulation(
            self.env,
            CONFIG,
            Telescope,
            planning_model=SHADOWPlanning('heft'),
            planning_algorithm='heft',
            scheduling=DynamicAlgorithmFromPlan,
            delay=None,
            timestamp=ts,
            to_file=True,
            hdf5_path=self.output
        )

        simulation.start(runtime=60)
        self.assertTrue(os.path.exists(self.output))

        store = pd.HDFStore(self.output)

        store[f'd{ts}/standard_simulation/sim']

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
            scheduling=BatchProcessing,
            delay=None,
            timestamp=f'{cwd}/test/data/output/{0}'
        )
        sim, task = simulation.start()
        self.assertGreater(len(sim), 0)


LARGE_CONFIG = 'test/data/config/mos_sw10.json'


@unittest.skip
class TestSimulationLargeExperiment(unittest.TestCase):
    def setUp(self) -> None:
        env = simpy.Environment()
        self.simulation = Simulation(
            env,
            LARGE_CONFIG,
            Telescope,
            planning_algorithm='heft',
            planning_model=SHADOWPlanning('heft'),
            scheduling=DynamicAlgorithmFromPlan,
            delay=None,
            timestamp=f'test/basic-workflow-data/output/{0}'
        )

    def testSimulationRuns(self):
        """
        This is a quick test to make sure that we can simulate a large
        complex simulation where all the moving pieces exist.

        There are a couple of conditions that require us to simply ignore an
        allocation; there are circumstances that exist when an observation
        provisions resources for ingest at the same timestep as allocations
        are made by the scheduler. The observation/ingest is provision first,
        but the resources are not 'consumed' (moved from 'available to
        unavailable') until after the scheduler has seen that the resources are
        available and assigned tasks to them.

        This may be fixed in future approaches by using a 'shadow' allocation
        scheme, which acts as a way of maintaining more global state with the
        actors still 'theoretically' blind. The current approach is useful in
        that it prototypes the use of simpy returns in a useful way.

        Returns
        -------

        """
        self.simulation.start()

    # BASIC WORKFLOW DATA


BASIC_CONFIG = f'{cwd}test/basic-workflow-data/basic_simulation.json'


class TestSimulationBasicSetup(unittest.TestCase):

    def setUp(self) -> None:
        env = simpy.Environment()
        self.simulation = Simulation(
            env,
            BASIC_CONFIG,
            Telescope,
            planning='heft',
            scheduling=DynamicAlgorithmFromPlan,
            delay=None,
            timestamp=f'test/basic-workflow-data/output/{0}'
        )

    def tearDown(self):
        output = f'test/basic-workflow-data/output/{0}'
        os.remove(f'{output}-sim.pkl')
        os.remove(f'{output}-tasks.pkl')
