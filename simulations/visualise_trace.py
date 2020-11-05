# Copyright (C) 10/9/20 RW Bunney

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


import json
import seaborn as sns

tracefile = 'test/data/output/sim.trace'

with open(tracefile, 'r') as infile:
	trace = json.load(infile)

for timestamp in trace:
	print('Time @ {}'.format(timestamp['timestamp']))
	print('\tcluster_state:')
	for element in timestamp['cluster_state']:
		for m in timestamp['cluster_state']['machines']:
			print('\t\t{}'.format(m))
	print('\ttelescope_state:')
	for element in timestamp['telescope_state']:
		print('\t\t{}: {}'.format(
			element, timestamp['telescope_state'][element]
		))
	print('\tscheduler_state:')
	print('\t\t{}'.format(timestamp['scheduler_state']))
	print('\t{}'.format(timestamp['buffer_state']))
	for element in timestamp['buffer_state']:
		print('\t\t{}'.format(timestamp['buffer_state'][element]))

