# Copyright (C) 25/3/21 RW Bunney

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

import os
import unittest
import simpy
import datetime
import pandas as pd

from pathlib import Path
from topsim.core.simulation import Simulation
from topsim.user.schedule.dynamic_plan import DynamicSchedulingFromPlan
from topsim.user.telescope import Telescope
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.plan.batch_planning import BatchPlanning  # Planning

CONFIG = Path("test/data/config/heft_single_observation_simulation.json")
# SIM_TIMESTAMP = f'test/simulation_pickles/{0}'
cwd = os.getcwd()


class TestMonitorPandasPickle(unittest.TestCase):

    def setUp(self) -> None:
        """
        Basic simulation using a single observation + heft workflow +
        homogenous system configuration.
        Returns
        -------
        """
        env = simpy.Environment()
        self.instrument = Telescope
        self.simulation = Simulation(
            env=env,
            config=CONFIG,
            instrument=self.instrument,
            planning_model=SHADOWPlanning('heft'),
            scheduling=DynamicSchedulingFromPlan(),
            delay=None,
            timestamp=0,
            to_file=True,
            hdf5_path=Path('test/simulation_data/test_hdf5.h5'),
            delimiters=f'test/'
        )

    def tearDown(self):
        output = f'{cwd}/test/simulation_pickles/{0}'
        os.remove('test/simulation_data/test_hdf5.h5')
        # os.remove(f'{output}-sim.pkl')
        # os.remove(f'{output}-tasks.pkl')

    def testHDF5GeneratedAfterSimulation(self):
        """
        Test that after a simulation, a HDF5 storage file is generated

        """
        self.simulation.start()
        self.assertTrue(os.path.exists('test/simulation_data/test_hdf5.h5'))

    def testHDF5KeysAndDataFramesExist(self):
        """
        Ensure that the generated HDF5 contains the correct results in the
        keys

        """

    def test_multi_simulation_data_merge(self):
        # global_sim_df = pd.DataFrame()
        # global_task_df = pd.DataFrame()

        for algorithm in ['heft', 'fcfs']:
            env = simpy.Environment()
            simulation = Simulation(
                env,
                CONFIG,
                self.instrument,
                planning_model=SHADOWPlanning(algorithm),
                scheduling=DynamicSchedulingFromPlan(ignore_ingest=True),
                delay=None,
                timestamp=0,
                hdf5_path='test/simulation_data/test_hdf5.h5',
                to_file=True,
                delimiters=f'{algorithm}'
            )
            simulation.start()
        self.assertTrue(
            os.path.exists('test/simulation_data/test_hdf5.h5')
        )
        # Necessary to get local datetime, so it passes tests everywhere
        timestamp = datetime.datetime.fromtimestamp(0).strftime('%a%y%m%d%H%M%S')
        heft_key = f'/{timestamp}/heft/heft_single_observation_simulation/sim/'
        fcfs_key = f'/{timestamp}/fcfs/heft_single_observation_simulation/sim/'
        heft_sim = pd.read_hdf(
            'test/simulation_data/test_hdf5.h5', key=heft_key
        )
        self.assertEqual(108, len(heft_sim))
        self.assertEqual(3, heft_sim.iloc[-1]['available_resources'])


class TestMonitorNoFileOption(unittest.TestCase):

    def setUp(self) -> None:
        """
        Basic simulation using a single observation + heft workflow +
        homogenous system configuration.
        Returns
        -------
        """
        self.env = simpy.Environment()
        self.instrument = Telescope
        # self.ts = f'{cwd}/test/simulation_pickles/{0}'

    def test_simulation_nofile_option(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning_model=SHADOWPlanning('heft'),
            scheduling=DynamicSchedulingFromPlan(),
            delay=None,
            timestamp=None,
        )
        simdf, taskdf = simulation.start()
        self.assertEqual(105, len(simdf))
        self.env = simpy.Environment()
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning_model=SHADOWPlanning('fcfs'),
            scheduling=DynamicSchedulingFromPlan(),
            delay=None,
            timestamp=None,
            # delimiters=f'test/{algorithm}'
        )
        simdf, taskdf = simulation.start()
        self.assertEqual(135, len(simdf))
        self.env = simpy.Environment()
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning_model=BatchPlanning('batch'),
            scheduling=BatchProcessing(min_resources_per_workflow=1, resource_split={},
                                       max_resource_partitions=1),
            delay=None,
            timestamp=None,
            # delimiters=f'test/{algorithm}'
        )
        simdf, taskdf = simulation.start()
        print(len(simdf))

    def testResultsAgreeWithExpectations(self):
        pass

    def testResultsWithLowDegreeDelays(self):
        delay_pickle = f'test/simulation_pickles/sim_nodelay'
        pass
