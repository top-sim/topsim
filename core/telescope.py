# import simpy
# from core.planner import Planner
# import config_data
import logging
# CHANGE THIS TO GET DEBUG VALUES FROM LOGS
import json

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
			self, env, telescope_config, planner, scheduler
	):
		self.env = env
		try:
			self.total_arrays, self.observations = process_telescope_config(
				telescope_config
			)
		except OSError:
			raise
		self.scheduler = scheduler
		self.observation_types = None
		self.total_arrays = None
		self.telescope_status = False
		self.telescope_use = 0
		self.planner = planner

	def run(self):
		while self.has_observations_to_process():
			for observation in self.observations:
				capacity = self.total_arrays - self.telescope_use
				# IF there is an observation ready for start
				if observation.is_ready(self.env.now, capacity):
					logger.info(
						'Observation %s scheduled for %s',
						observation.name,
						self.env.now
					)
				if self.scheduler.ingest_capacity(observation):
					observation.status = self.begin_observation(observation)
					plan_trigger = self.env.process(
						self.planner.run(observation)
					)
					yield plan_trigger
					logger.info(
						'Telescope is now using %s arrays', self.telescope_use
					)
				# If an observation is running and we want to stop it
				elif observation.is_finished(
						self.env.now,
						self.telescope_status
				):
					observation.status = self.finish_observation(observation)
					logger.info(
						'Telescope is now using %s arrays', self.telescope_use
					)

		yield self.env.timeout(1)

	def begin_observation(self, observation):
		self.telescope_use += observation.demand
		self.telescope_status = True
		return RunStatus.RUNNING

	def finish_observation(self, observation):
		self.telescope_use -= observation.demand

		if self.telescope_use is 0:
			self.telescope_status = False
		return RunStatus.FINISHED

	def run_observation_on_telescope(self, demand):
		pass

	def has_observations_to_process(self):
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
	Observation object stores information about a given observation;
	the object also stores information about the workflow, and the generated
	plan for that workflow.
	"""

	def __init__(self, name, start, duration, demand, workflow, type,
				 data_rate):
		self.name = name
		self.start = start
		self.duration = duration
		self.demand = demand
		self.status = RunStatus.WAITING
		self.type = type
		self.workflow = workflow
		self.total_data_size = 0
		self.ingest_data_rate = data_rate
		self.type = None
		self.plan = None

	def is_ready(self, current_time, capacity):
		if self.start <= current_time \
				and self.demand <= capacity \
				and self.status is RunStatus.WAITING:
			return True
		else:
			return False

	def is_finished(self, current_time, telescope_status):
		if current_time > self.start + self.duration \
				and telescope_status \
				and (self.status is not RunStatus.FINISHED):
			return True
		else:
			return False


def process_observation_template(config_dict):
	"""
	Read in observation dictionary
	:return: Observation object

	"""
	observation = config_dict

	return observation

def process_telescope_config(telescope_config):
	try:
		with open(telescope_config, 'r') as infile:
			config = json.load(infile)
	except OSError:
		logger.warning("File %s not found", telescope_config)
		raise
	except json.JSONDecodeError:
		logger.warning("Please check file is in JSON Format")
		raise
	try:
		'telescope' in config and 'observation' in config
	except KeyError:
		logger.warning(
			"'telescope/observation' is not in %s, "
			"check your JSON is correctly formatted",
			config
		)
		raise
	total_arrays = config['telescope']['total_arrays']
	observations = []
	for observation in config['observations']:
		try:
			type = observation['type']
			o = Observation(
				name=observation['name'],
				start=observation['start'],
				duration=observation['duration'],
				demand=observation['demand'],
				workflow=observation['workflow'],
				type = observation['type'],
				data_rate=observation['data_produce_rate']
			)
			observations.append(o)
		except KeyError:
			raise

	return total_arrays, observations


# {
# 	"name": "emu",
# 	"start": 0,
# 	"duration": 10,
# 	"demand": 36,
# 	"workflow": "continuum",
# 	"data_product_rate": 4
# },



class RunStatus(str, Enum):
	WAITING = 'WAITING'
	RUNNING = 'RUNNING'
	FINISHED = 'FINISHED'
