# Copyright (C) 19/11/20 RW Bunney

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


"""
Running a simulation with observations of a significant size - and therefore,
time delay on storing and ingest.

This also involves the planning and execution of a more complex workflow;
namely, the original HEFT workflow from Topcuoglu 2000.
"""

import simpy
import logging

from topsim.user.schedule.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.telescope import Telescope
from topsim.core.simulation import Simulation
from topsim.core.delay import DelayModel

# logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)

CONFIG = 'simulations/heft_sim/input/standard_simulation.json'
env = simpy.Environment()
planning_algorithm = 'heft'
scheduling_algorithm = DynamicAlgorithmFromPlan
instrument = Telescope
dm = DelayModel(0.5, 'normal', DelayModel.DelayDegree.LOW)

simulation = Simulation(
    env=env,
    config=CONFIG,
    instrument=instrument,
    planning_algorithm='heft',
    planning_model=SHADOWPlanning('heft'),
    scheduling=DynamicAlgorithmFromPlan(),
    delay=None,
    timestamp='heft_sim',
    to_file=False
)

simulation.start()
# simulation.resume(200)


"""
            env = simpy.Environment()
            instrument = Telescope
            # timestamp = f'{time.time()}'.split('.')[0]
            simulation = Simulation(
                env=env,
                config=cfg,
                instrument=instrument,
                planning_algorithm='heft',
                planning_model=SHADOWPlanning('heft'),
                scheduling=DynamicAlgorithmFromPlan,
                delay=None,
                timestamp='skaworkflows_test',
                to_file=True,
                hdf5_path=f'{RUN_PATH}/{FOLDER_PATH}/results.h5'
                # hdf5_path='',
                # delimiters=f'test/'
            )
            simulation.start(runtime=60)
            simulation.resume(100)

"""