# Copyright (C) 28/7/20 RW Bunney

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
Basic simulation, with minimal Observation Plan and dummy observation workflows
"""

import simpy
import logging

from topsim.algorithms.scheduling import FifoAlgorithm
from topsim.core.simulation import Simulation

logging.basicConfig(level="WARNING")
LOGGER = logging.getLogger(__name__)

BASIC_WORKFLOW = 'simulations/basic_sim/input/basic_workflow_config.json'
BASIC_CLUSTER = 'simulations/basic_sim/input/basic_config.json'
BASIC_BUFFER = 'simulations/basic_sim/input/basic_buffer.json'
BASIC_TELESCOPE = 'simulations/basic_sim/input/basic_telescope.json'

EVENT_FILE = 'simulations/basic_sim/output/sim.trace'

# env = simpy.RealtimeEnvironment(factor=0.5, strict=False)
env = simpy.Environment()
event_file = EVENT_FILE
planning_algorithm = 'heft'
scheduling_algorithm = FifoAlgorithm()
simulation = Simulation(
	env,
	BASIC_TELESCOPE,
	BASIC_CLUSTER,
	BASIC_BUFFER,
	planning_algorithm,
	scheduling_algorithm,
	EVENT_FILE,
)

simulation.start(16)
simulation.resume(17)
