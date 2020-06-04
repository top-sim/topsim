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

import logging
from queue import PriorityQueue
import config

logger = logging.getLogger(__name__)

BUFFER_OFFSET = config.BUFFER_TIME_OFFSET

class BufferQueue():
	def __init__(self):
		self._queue = []

	def push(self, x):
		self._queue.append(x)

	def pop(self):
		return self._queue.pop(0)

	def size(self):
		return len(self._queue)

	def empty(self):
		return len(self._queue) == 0


class Buffer(object):
	def __init__(self, env, cluster):
		self.env = env
		self.cluster = cluster
		self.observations_for_processing = BufferQueue()
		self.waiting_observation_list = []
		self.workflow_plans = {}
		self.new_observation = 0

	def run(self, observation):
		logger.debug("Attempting to add observation %s to buffer", observation.name)
		# logger.info("Observation %s placed in buffer at ", observation.name, self.env.now)
		self.add_observation_to_buffer(observation)
		yield self.env.timeout(0)
		# Reminder that observations_for_processing has Observation objects.
		# self.observations_for_processing.remove(obs)

	def add_observation_to_buffer(self, observation):
		logger.info("Adding observation %s data to buffer at time %s", observation.name, self.env.now)
		observation.plan.start_time = self.env.now + BUFFER_OFFSET
		# self.process_observation_plan_for_scheduling(observation)
		self.waiting_observation_list.append(observation)
		logger.debug('Observations in buffer %', self.waiting_observation_list)

# TODO Buffer needs more specifications - data transfer times/latency/bandwidth
# TODO Need specification on buffer makeup

