# Copyright (C) 1/9/20 RW Bunney

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
This script runs a default simulation, with no 'bells and whistles'.
It uses a standard scheduling and planning model with no failures,
and a homogneoues cluster system.

The only requirements are the SHADOW library implementations of static
scheduling algorithms for the Planner interface, Simpy, and numby/matplotlib.
"""

import simpy

from topsim.core.simulation import Simulation

from topsim.algorithms.dynamic_plan import FifoAlgorithm

# We need a number of configuration files for the basic set-up

TELESCOPE_CONFIG = 'test/data/config/observations.json'
CLUSTER_CONFIG = 'test/data/config/basic_spec-10.json'
BUFFER_CONFIG = 'test/data/config/buffer.json'
EVENT_FILE = 'recipes/output/sim.trace'

# Planning Algorithm - needs to be in our external library
planning_algorithm = 'heft'

# Scheduling Algorithm - these are implemented in a specific format to work
# in conjunction with the Scheduler Actor
scheduling_algorithm = FifoAlgorithm()

# Inititalise Simpy environment
env = simpy.Environment()

simulation = Simulation(
    env,
    TELESCOPE_CONFIG,
    CLUSTER_CONFIG,
    BUFFER_CONFIG,
    planning_algorithm,
    scheduling_algorithm,
    EVENT_FILE,
    visualisation=False
)
