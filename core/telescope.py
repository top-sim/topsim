# import simpy
# from core.planner import Planner
# import config_data
import logging
# CHANGE THIS TO GET DEBUG VALUES FROM LOGS

from enum import Enum

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class TelescopeQueue:

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


class Telescope:
	def __init__(
			self, env, observations, buffer_obj, telescope_config, planner
	):
		self.env = env
		self.observations = observations
		self.buffer = buffer_obj
		self.telescope_status = False
		self.telescope_use = 0
		self.max_array_use = telescope_config
		self.planner = planner

	def run(self):
		while self.observations_to_process():
			for observation in self.observations:
				capacity = self.max_array_use - self.telescope_use
				# TODO INSERT IF CONDITION COMMENT
				if observation.is_ready(self.env.now, capacity):
					logger.info(
						'Observation %s scheduled for %s',
						observation.name,
						self.env.now
					)
					self.telescope_use += observation.demand
					self.telescope_status = True
					observation.status = RunStatus.RUNNING
					plan_trigger = self.env.process(
						self.planner.run(observation)
					)
					yield plan_trigger
					logger.info(
						'Telescope is now using %s arrays',
						self.telescope_use)

				# TODO INSERT ELIF CONDITION COMMENT
				elif self.env.now > observation.start + observation.duration \
					and self.telescope_status \
					and (observation.status is not RunStatus.FINISHED):

					self.telescope_use -= observation.demand
					logger.info('Telescope is now using %s arrays',
								self.telescope_use)
					if self.telescope_use is 0:
						self.telescope_status = False
					observation.status = RunStatus.FINISHED

		yield self.env.timeout(1)

	def run_observation_on_telescope(self, demand):
		pass

	def observations_to_process(self):
		for observation in self.observations:
			if observation.status == RunStatus.FINISHED:
				continue
			else:
				return True
		return False

	def print_state(self):
		return {
			'telescope_in_use': self.telescope_status,
			'telescope_arrays_used': self.telescope_use,
			'observations_waiting': len(self.observations)
		}


class Observation(object):
	"""
	Observation object stores information about a given observation; the object also
	stores information about the workflow, and the generated plan for that workflow.
	"""

	def __init__(self, name, start, duration, demand, workflow):
		self.name = name
		self.start = start
		self.duration = duration
		self.demand = demand
		self.status = RunStatus.WAITING
		self.workflow = workflow
		self.total_data_size = 0
		self.ingest_data_rate = None
		self.type = None
		self.plan = None

	def is_ready(self, current_time, capacity):
		if self.start <= current_time \
				and self.demand <= capacity \
				and self.status is RunStatus.WAITING:
			return True
		else:
			return False

	def process_observation_template(self, filename):
		"""
		Read in observation plan outline
		:return: True if JSON is passed correctly
		"""
		return True


class ObservationType(Enum):
	CONTINUUM = 'CONTINUUM'
	SECTRAL = 'SPECTRAL'
	PULSAR = 'PULSAR'
	TRANSIENT = 'TRANSIENT'


class RunStatus(str, Enum):
	WAITING = 'WAITING'
	RUNNING = 'RUNNING'
	FINISHED = 'FINISHED'

