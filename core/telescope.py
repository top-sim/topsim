# import simpy
# from core.planner import Planner
# import config_data
import logging
# CHANGE THIS TO GET DEBUG VALUES FROM LOGS

from enum import Enum

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class Telescope(object):
	def __init__(self, env, observations, buffer_obj, telescope_config, planner):
		self.env = env
		self.observations = observations  # .sort(key=lambda x: x.start)
		self.buffer = buffer_obj
		# self.run_observation = self.env.process(self.run(env))
		self.telescope_status = False
		self.telescope_use = 0
		self.max_array_use = telescope_config
		self.planner = planner

	def run(self):
		while self.check_observation_status():
			for observation in self.observations:
				capacity = self.max_array_use-self.telescope_use
				if observation.is_ready(self.env.now, capacity):
					logger.info('Observation %s scheduled for %s',observation.name, self.env.now)
					self.telescope_use += observation.demand
					self.telescope_status = True
					observation.status = RunStatus.RUNNING
					plan_trigger = self.env.process(self.planner.run(observation))
					yield plan_trigger
					logger.info('Telescope is now using %s arrays', self.telescope_use)
				elif self.env.now > observation.start + observation.duration and self.telescope_status and (observation.status is not RunStatus.FINISHED) :

					self.telescope_use -= observation.demand
					logger.info('Telescope is now using %s arrays', self.telescope_use)
					# print('Telescope is now using', self.telescope_use, 'arrays')
					if self.telescope_use is 0:
						self.telescope_status = False
					observation.status = RunStatus.FINISHED

				# else:
				# 	# print("Nothing to do for ", observation.name, self.env.now)
			# logger.info('Time is %s', self.env.now)
			yield self.env.timeout(1)

	def run_observation_on_telescope(self, demand):
		pass

	def start_ingest_pipelines(self, observation):
		"""
		Ingest is 'streaming' data to the buffer during the observation
		How we calculate how long it takes remains to be seen
		For the time being, we will be doubling the observation time
		"""
		streaming_time = observation.duration*2
		if self.buffer.check_buffer_capacity(observation.project_output):
			yield self.env.timeout(streaming_time)
		else:
			return False
		buffer_trigger = self.env.process(self.buffer.run(observation))
		yield buffer_trigger

	def check_observation_status(self):
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
		self.data_output = 0
		self.type = None
		self.plan = None

	def is_ready(self, current_time, capacity):
		if self.start <= current_time \
			and self.demand <= capacity \
			and self.status is RunStatus.WAITING:
			return True
		else:
			return False


class ObservationType(Enum):
	CONTINUUM = 'CONTINUUM'
	SECTRAL = 'SPECTRAL'
	PULSAR = 'PULSAR'
	TRANSIENT = 'TRANSIENT'


class RunStatus(str, Enum):
	WAITING = 'WAITING'
	RUNNING = 'RUNNING'
	FINISHED = 'FINISHED'


#
#
# if __name__ == '__main__':
#
#     emu = Observation('emu', 0, 10, 46)
#     dingo = Observation('dingo', 10, 15, 18)
#     vast = Observation('vast', 20, 30, 18)
#
#     tconfig = 36  # for starters, we will define telescope configuration as simply number of arrays that exist
#     # [start_time, duration, num_arrays_used]
#     observation_data = [emu, dingo, vast]  # , [40, 10, 36]]
#
#     simenv = simpy.environment()
#     buffer = Buffer(simenv)
#
#     telescope = Telescope(simenv, observation_data, buffer, tconfig)
#     simenv.run()
