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

import pandas as pd

from topsim.core.simulation import Simulation
from topsim.user.schedule.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.telescope import Telescope
from topsim.user.plan.static_planning import SHADOWPlanning

CONFIG = "test/simulation_pickles/heft_single_observation_simulation.json"

SIM_TIMESTAMP = f'test/simulation_pickles/{0}'
EVENT_PICKLE = f'{SIM_TIMESTAMP}-heft-GreedyAlgorithmFromPlan-sim.pkl'
TASKS_PICKLE = f'{SIM_TIMESTAMP}-heft-GreedyAlgorithmFromPlan-tasks.pkl'

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
        instrument = Telescope
        self.simulation = Simulation(
            env=env,
            config=CONFIG,
            instrument=instrument,
            planning_algorithm='heft',
            planning_model=SHADOWPlanning('heft'),
            scheduling=DynamicAlgorithmFromPlan,
            delay=None,
            timestamp=SIM_TIMESTAMP,
            to_file=True
        )

    def tearDown(self):
        output = f'{cwd}/test/simulation_pickles/{0}'
        os.remove(f'{output}-sim.pkl')
        os.remove(f'{output}-tasks.pkl')

    def testPickleGeneratedAfterSimulation(self):
        res = self.simulation.start(-1)

        sim_df = pd.read_pickle('test/simulation_pickles/0-sim.pkl')
        tasks_df = pd.read_pickle('test/simulation_pickles/0-tasks.pkl')
        self.assertTrue('running_tasks' in sim_df)


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
        self.ts = f'{cwd}/test/simulation_pickles/{0}'

    def test_simulation_nofile_option(self):
        simulation = Simulation(
            self.env,
            CONFIG,
            self.instrument,
            planning_algorithm='heft',
            planning_model=SHADOWPlanning('heft'),
            scheduling=DynamicAlgorithmFromPlan,
            delay=None,
            timestamp=self.ts,
        )
        simdf, taskdf = simulation.start(runtime=60)
        self.assertFalse(os.path.exists("test/simulation_pickles/0-sim.pkl"))

    # @unittest.skip("testing")
    def test_multi_siulation_data_merge(self):
        global_sim_df = pd.DataFrame()
        global_task_df = pd.DataFrame()
        for algorithm in ['heft', 'fcfs']:
            env = simpy.Environment()
            simulation = Simulation(
                env,
                CONFIG,
                self.instrument,
                planning_algorithm=algorithm,
                planning_model=SHADOWPlanning(algorithm),
                scheduling=DynamicAlgorithmFromPlan,
                delay=None,
                timestamp=self.ts,
            )
            simdf, taskdf = simulation.start()
            global_sim_df = global_sim_df.append(simdf)
            global_task_df = global_task_df.append(taskdf)
        self.assertEqual(246, len(global_sim_df))

    def testResultsAgreeWithExpectations(self):
        pass

    def testResultsWithLowDegreeDelays(self):
        delay_pickle = f'test/simulation_pickles/sim_nodelay'
        pass
