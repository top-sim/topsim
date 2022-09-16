# Copyright (C) 2022 RW Bunney

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
Use configuration from heft_sim.py to demonstrate batch allocation model
"""

import simpy
import logging

from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.plan.batch_planning import BatchPlanning
from topsim.user.telescope import Telescope
from topsim.core.simulation import Simulation
from topsim.core.delay import DelayModel

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)

CONFIG = 'examples/heft_single_observation_simulation.json'
env = simpy.Environment()
planning_algorithm = 'batch'
scheduling_algorithm = BatchProcessing
instrument = Telescope
dm = DelayModel(0.5, 'normal', DelayModel.DelayDegree.LOW)

simulation = Simulation(
    env=env,
    config=CONFIG,
    instrument=instrument,
    planning_algorithm=planning_algorithm,
    planning_model=BatchPlanning('batch'),
    scheduling=BatchProcessing(min_resources_per_workflow=1,resource_split={'emu':(1,3)}),
    delay=None,
    timestamp='heft_sim',
    to_file=False
)

simulation.start()

env = simpy.Environment()
simulation = Simulation(
    env=env,
    config=CONFIG,
    instrument=instrument,
    planning_algorithm=planning_algorithm,
    planning_model=BatchPlanning('batch'),
    scheduling=BatchProcessing(),
    delay=None,
    timestamp='heft_sim',
    to_file=False
)
simulation.start()