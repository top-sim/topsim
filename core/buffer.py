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
from common import config

from core.telescope import RunStatus
import common.config

logger = logging.getLogger(__name__)

BUFFER_OFFSET = common.config.BUFFER_TIME_OFFSET


class BufferQueue:
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
	def __init__(self, env, cluster, config):
		self.env = env
		self.cluster = cluster
		# TODO split the hot/cold buffer into separate objects/tuples?
		self.hot_total_capacity = 0
		self.hot,self.cold = config.process_buffer_config(config)
		self.cold_total_capacity = 0
		self.cold = None
		self.hardware = {}
		self.observations_for_processing = BufferQueue()
		self.waiting_observation_list = []
		self.workflow_plans = {}
		self.new_observation = 0


	def run(self):
		while True:
			if self.cluster.ingest:
				observation = self.cluster.ingest_observation
				logger.debug(
					"Attempting to add observation %s to buffer",
					observation.name)
				observation.data = self.ingest_data_stream(observation)
				# Observation is only 'added_to_buffer' once the data
				# has been completely added
				if observation.status == RunStatus.FINISHED:
					self.add_observation_to_buffer(observation)
			yield self.env.timeout(1)

	def check_buffer_capacity(self, observation):
		if self.hot - observation.size < 0 \
			and self.cold - observation.size < 0:
			return False
		else:
			return True

	def ingest_data_dump(self, data):
		pass

	def add_observation_to_buffer(self, observation):
		logger.info(
			"%s data to buffer at time %s", observation.name, self.env.now
		)
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

	def ingest_data_stream(self, observation):
		"""
		Buffer ingests the data stream from the Ingest pipelines. the data
		is what is added to the 'hot' buffer every timestep
		That is - the observation.ingest_data_rate is a per-timestep value
		:param observation: The observation that is being conducted(?)
		:return: True if the buffer 'Accepts' the data
		"""
		while observation.status == RunStatus.RUNNING:
			hot_buffer_accepts = self.hot.process_incoming_data_stream(
				observation.ingest_data_rate
			)
			yield hot_buffer_accepts



class HotBuffer:
	def __init__(self, capacity, max_ingest_data_rate):
		self.total_capacity = capacity
		self.current_capacity = self.total_capacity
		self.max_ingest_data_rate = max_ingest_data_rate

	def process_incoming_data_stream(self, incoming_datarate):
		"""
		During Ingest, the buffer will coordinate the incoming data from
		the observation. This is a sanity check function to make sure the
		hot buffer has capacity to accept the incoming data.

		:param incoming_datarate: The amount of data-per-timestep the telescope
		is producing
		:return: True if the data can be processed - false if it cannot
		"""
		if incoming_datarate > self.max_ingest_data_rate:
			raise ValueError(
				'Incoming data rate {0} exceeds maximum.'.format(
					incoming_datarate)
			)
		if self.current_capacity - incoming_datarate <0:
			return False
		else:
			self.current_capacity - incoming_datarate
			return True


	def cold_buffer_data_request(self, observation):
		"""
		The cold buffer will request data from the observation
		:param The observation for which the data is being requested
		:return: The
		"""


class ColdBuffer:
	def __init__(self,size, data_rate):
		"""
		The ColdBuffer takes data from the hot buffer for use in workflow
		processing
		"""


# TODO Buffer needs more specifications - data transfer times/latency/bandwidth
# TODO Need specification on buffer makeup

