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

from topsim.algorithms.scheduling import FifoAlgorithm
from topsim.core.simulation import Simulation

CLUSTER_CONFIG = 'simulations/heft_sim/input/basic_spec-10.json'
HEFT_WORKFLOW = 'simulations/heft_sim/input/workflow_config.json'

OBSERVATION_CONFIG = 'simulations/heft_sim/input/single_observation_config.json'
BUFFER_CONFIG = 'simulations/heft_sim/input/buffer.json'

EVENT_FILE = 'simulations/heft_sim/output/heft_sim.trace'

env = simpy.Environment()

planning_algorithm = 'heft'
scheduling_algorithm = FifoAlgorithm()

simulation = Simulation(
    env,
    OBSERVATION_CONFIG,
    CLUSTER_CONFIG,
    BUFFER_CONFIG,
    planning_algorithm,
    scheduling_algorithm,
    EVENT_FILE,
)

simulation.start(11)
simulation.resume(115)
