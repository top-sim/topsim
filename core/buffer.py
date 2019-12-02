from core.job import Job
from collections import deque


class Buffer(object):
	def __init__(self, env, cluster):
		self.env = env
		self.cluster = cluster
		self.destroyed = False
		# Observations_for_processing is a queue of observations; popleft() to get the oldest observation
		self.observations_for_processing = deque()

	def run(self, observation):
		print("Observation placed in buffer at ", self.env.now)
		print(observation.name)
		observation.plan.start_time = self.env.now
		self.add_observation_to_waiting_workflows(observation)
		# yield self.env.process(self.simulation.task_broker.run())
		# yield self.env.timeout(0)
		# Reminder that observations_for_processing has Observation objects.
		if self.observations_for_processing:
			print("Workflows currently waiting in the Buffer: {0}".format(self.observations_for_processing))
			obs = self.observations_for_processing.popleft()
			assert obs.plan.start_time >= self.env.now
			yield self.env.timeout(obs.plan.start_time - self.env.now)
			# print('a task arrived at time %f' % self.env.now)
			self.cluster.add_workflow_plan(obs.plan)
			# self.observations_for_processing.remove(obs)
		# self.destroyed = True

	def add_observation_to_waiting_workflows(self, observation):
		print("Adding", observation.name, "to workflows")
		self.observations_for_processing.append(observation)
		print("Waiting workflows", self.observations_for_processing)

	# def attach(self, simulation):
	# 	self.simulation = simulation
	# 	self.cluster = simulation.cluster

# class Broker(object):
# 	def __init__(self, env, job_configs):
# 		self.env = env
# 		self.simulation = None
# 		self.cluster = None
# 		self.destroyed = False
# 		self.job_configs = job_configs
# 		self.observations_for_processing = []
#
# 	def attach(self, simulation):
# 		self.simulation = simulation
#
# 	def run(self):
#
	"""
	The broker is going to accumulate workflows over time. At each time point, it 
	is going to check if it has any new workflows; if it does, it will go and fetch
	the information about that workflow (intended start time of first task etc.). This 
	expected start time is produced by the planner, which should take into account 
	the expected exit time of the observation, and a 'buffer delay' that is specified at 
	the start of the simulation.  
	"""