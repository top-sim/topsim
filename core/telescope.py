import simpy
from core.simulation import Simulation
from core.planner import Planner
import config_data

class Telescope(object):

	def __init__(self, env, observations, buffer_obj, telescope_config, planner):
		self.simulation = None
		self.env = env
		self.observations = observations  # .sort(key=lambda x: x.start)
		self.buffer = buffer_obj
		# self.run_observation = self.env.process(self.run(env))
		self.telescope_status = False
		self.telescope_use = 0
		self.max_array_use = telescope_config
		self.planner = planner

	def attach(self, simulation):
		self.simulation = simulation
		self.buffer.attach(simulation)

	def run(self):
		while self.observations:
			for observation in self.observations:
				if observation.start <= self.env.now \
					and observation.demand <= self.max_array_use-self.telescope_use \
					and not observation.running: \
					# and not obs_schedule_status:
					print('Observation', observation.name, 'scheduled', self.env.now)
					self.telescope_use += observation.demand
					self.telescope_status = True
					observation.running = True
					self.env.process(self.planner.run(observation))
					print('Telescope is now using', self.telescope_use, 'arrays')
				elif self.env.now > observation.start + observation.duration and self.telescope_status:
					buffer_trigger = self.env.process(self.buffer.run(observation.name))
					yield buffer_trigger
					self.telescope_use -= observation.demand
					print('Telescope is now using', self.telescope_use, 'arrays')
					if self.telescope_use is 0:
						self.telescope_status = False
					self.observations.remove(observation)
				else:
					print("Nothing to do for ", observation.name, self.env.now)
			print(self.env.now)
			yield self.env.timeout(1)
			# print('Time now is ', self.env.now)

	@property
	def state(self):
		return {
			'telescope_in_use': self.telescope_status,
			'telescope_arrays_used': self.telescope_use,
			'observations_waiting': len(self.observations)
		}


class Workflow(object):
	"""
	Workflow Object is used to store basic information about the workflow.
	The workflow is specified in a file with name 'workflow', and then read into the simulator.
	"""
	def __init__(self, workflow):
		self.id = workflow


class Observation(object):

	def __init__(self, name, start, duration, demand, workflow):
		self.name = name
		self.start = start
		self.duration = duration
		self.demand = demand
		self.running = False
		self.completed = False
		self.workflow = workflow


class Buffer(object):

	def __init__(self, environ):
		self.env = environ
		self.simulation = None

	def run(self, observation_workflow):
		print("Observation placed in buffer at ", self.env.now)
		print(observation_workflow)
		self.simulation.task_broker.add_workflow(observation_workflow)
		yield self.env.process(self.simulation.task_broker.run())
		# yield self.env.timeout(0)

	def attach(self, simulation):
		self.simulation = simulation
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
