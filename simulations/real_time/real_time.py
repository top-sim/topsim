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

from topsim.user.dynamic_plan import DynamicAlgorithmFromPlan
from topsim.user.telescope import Telescope
from topsim.core.simulation import Simulation

EVENT_FILE = 'simulations/real_time/real_time.trace'
CONFIG = 'simulations/real_time/real_time.json'

env = simpy.Environment()

planning_algorithm = 'heft'
scheduling_algorithm = DynamicAlgorithmFromPlan
instrument = Telescope

simulation = Simulation(
    env=env,
    config=CONFIG,
    instrument=instrument,
    algorithm_map={'pheft': 'pheft', 'heft': 'heft', 'fifo': DynamicAlgorithmFromPlan},
    event_file=EVENT_FILE,
)

simulation.start(11)
simulation.resume(300)