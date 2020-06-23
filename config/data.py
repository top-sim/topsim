# Copyright (C) 04/10/2019 RW Bunney

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

########################################################################

"""
Config Data for the simulation; the format of this will change, this is a stub
"""

# Simulation configuration for testing
telescope_config = 'test/data/observations.csv'
machine_config = 'test/data/system_config.json'
workflow_config = 'test/data/workflow_config.json'
event_file = '/sim.trace'
planning_algorithm = 'heft'

test_scheduler_workflow = 'test/data/workflow_config.json'
test_buffer_workflow = 'test/data/workflow_config.json'
test_plan_workflow = 'test/data/workflow_config.json'


# Extra time added to the start time of an observation workflow  when it enters the buffer
# This is to account for I/O and overheads
buffer_offset = 5


