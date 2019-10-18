from core.job import Job


class Broker(object):
	def __init__(self, env, job_configs):
		self.env = env
		self.simulation = None
		self.cluster = None
		self.destroyed = False
		self.job_configs = job_configs
		self.observations_for_processing = []

	def attach(self, simulation):
		self.simulation = simulation
		self.cluster = simulation.cluster

	def run(self):
		print("Broker is waiting")
		# Reminder that observations_for_processing has Observation objects.
		if self.observations_for_processing:
			print("Workflows currently waiting with the Broker: {0}".format(self.observations_for_processing))
		for observation in self.observations_for_processing:
			assert observation.plan.start_time >= self.env.now
			yield self.env.timeout(observation.plan.start_time - self.env.now)
			# print('a task arrived at time %f' % self.env.now)
			self.cluster.add_workflow_plan(observation.plan)
			self.observations_for_processing.remove(observation)
		# self.destroyed = True

	def add_observation_to_waiting_workflows(self, observation):
		print("Adding", observation.name, "to workflows")
		# Instatiate a new workflow object with the submission time
		self.observations_for_processing.append(observation)
		print("Waiting workflows", self.observations_for_processing)

	"""
	The broker is going to accumulate workflows over time. At each time point, it 
	is going to check if it has any new workflows; if it does, it will go and fetch
	the information about that workflow (intended start time of first task etc.). This 
	expected start time is produced by the planner, which should take into account 
	the expected exit time of the observation, and a 'buffer delay' that is specified at 
	the start of the simulation.  
	"""