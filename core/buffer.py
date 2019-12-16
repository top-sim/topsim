# Copyright (C) 10/19 RW Bunney

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

from queue import Queue
import config

BUFFER_OFFSET = config.BUFFER_TIME_OFFSET


class Buffer(object):
	def __init__(self, env, cluster):
		self.env = env
		self.cluster = cluster
		self.observations_for_processing = Queue()
		self.workflow_plans = {}

	def run(self, observation):
		print("Observation placed in buffer at ", self.env.now)
		print(observation.name)
		self.add_observation_to_waiting_workflows(observation)
		yield self.env.timeout(0)
		# Reminder that observations_for_processing has Observation objects.
		# self.observations_for_processing.remove(obs)

	def add_observation_to_waiting_workflows(self, observation):
		print("Adding", observation.name, "to workflows")
		observation.plan.start_time = self.env.now + BUFFER_OFFSET
		self.observations_for_processing.put(observation)
		print("Waiting workflows", self.observations_for_processing)



