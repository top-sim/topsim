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
from config import config

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
	def __init__(self, env, cluster, buffer_io):
		self.env = env
		self.cluster = cluster
		self.io = buffer_io
		self.hot = None
		self.cold = None
		self.hardware = {}
		self.observations_for_processing = BufferQueue()
		self.waiting_observation_list = []
		self.workflow_plans = {}
		self.new_observation = 0
		self.capacity = 0

	def run(self, observation):
		logger.debug("Attempting to add observation %s to buffer", observation.name)
		self.add_observation_to_buffer(observation)
		yield self.env.timeout(0)

	def check_buffer_capacity(self, data_product):
		if self.capacity - data_product.size < 0:
			return False
		else:
			return True

	def ingest_data_dump(self, data):
		pass

	def add_observation_to_buffer(self, observation):
		logger.info("Adding observation %s data to buffer at time %s", observation.name, self.env.now)
		# This will take time
		observation.plan.start_time = self.env.now + BUFFER_OFFSET
		self.waiting_observation_list.append(observation)
		logger.debug('Observations in buffer %', self.waiting_observation_list)

	def request_observation_data_from_buffer(self, observation):
		logger.info("Removing observation from buffer at time %s")
		data_transfer_time = 0
		# This will take time, so we need to timeout
		yield self.env.timeout(data_transfer_time)
		self.waiting_observation_list.remove(observation)
		# In the future we will be able to interrupt this
		return True

	def request_data(self, task):
		pass

	def add_data_to_buffer(self, data_object):
		dump_time = self._data_transfer_time(data_object)
		return dump_time

	def _data_transfer_time(self, data_object):
		pass

# TODO Buffer needs more specifications - data transfer times/latency/bandwidth
# TODO Need specification on buffer makeup



