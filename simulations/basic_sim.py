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

from algorithms.scheduling import FifoAlgorithm
from core.simulation import Simulation
import common.data as test_data


workflow_file = 'test/data/daliuge_pipeline_test.json'
event_file = 'sim.trace'
planning_algorithm = 'heft'
# env = simpy.RealtimeEnvironment(factor=0.5, strict=False)
env = simpy.Environment()
tmax = 36  # for starters, we will define telescope configuration as simply number of arrays that exist
salgorithm = FifoAlgorithm()
vis = False

# TODO move things like 'heft' into a 'common' file which has SchedulingAlgorithm.HEFT = 'heft' etc.
simulation = Simulation(
	env,
	test_data.telescope_config,
	tmax,
	test_data.machine_config,
	salgorithm,
	'heft',
	event_file,
	vis
)
simulation.start(-1)