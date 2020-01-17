# Copyright (C) 17/1/20 RW Bunney

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
Use 'bokeh' python library to visualise the current state of the system
"""

class Visualiser(object):
	def __init__(self, simulation):
		self.simulation = simulation
		self.env = self.simulation.env
		self.events = []

	def run(self):
		while not self.simulation.is_finished():
			# Visulaisation logic
			print(self.simulation.cluster)
			print(self.simulation.scheduler)
			print(self.simulation.telescope)
			print(self.simulation.planner)
			print(self.simulation.buffer)
			yield self.env.timeout(1)

	def write_to_file(self):
		with open(self.event_file, 'w') as f:
			json.dump(self.events, f, indent=4)


