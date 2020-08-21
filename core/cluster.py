import json
import copy
from core.machine import Machine,Status
from core import config
from queue import Queue, PriorityQueue

from core.planner import Task, TaskStatus


class Cluster(object):
	"""
	A class used to represent the Cluster, the abstract representation
	of computing resources in the Science Data Processor

	Attributes
	----------
	says_str : str
	    a formatted string to print out what the animal says
	name : str
	    the name of the animal
	sound : str
	    the sound that the animal makes
	num_legs : int
	    the number of legs the animal has (default 4)

	Methods
	-------
	says(sound=None)
	    Prints the animals name and what sound it makes
	"""

	def __init__(self, env, spec):
		"""

		:param env:
		:param spec:
		"""
		try:
			self.machines, self.system_bandwidth = \
				config.process_machine_config(spec)
		except OSError:
			raise
		self.dmachine = {machine.id: machine for machine in self.machines}
		self.available_resources = [machine for machine in self.machines]

		self.occupied_resources = []
		self.ingest_resources = []
		self.running_tasks = []
		self.finished_tasks = []
		self.waiting_tasks = []
		self.ingest = False
		self.finished_workflows = []
		self.ingest_pipeline = None
		self.ingest_obervation = None
		self.env = env

	def run(self):
		while True:
			if len(self.waiting_tasks) > 0:
				for task in self.waiting_tasks:
					if task.task_status is TaskStatus.FINISHED:
						self.waiting_tasks.remove(task)
						self.finished_tasks.append(task)
					if task.est >= self.env.now:
						machine = self.dmachine[task.machine.id]
						self.running_tasks.append(task)
						machine.run(task)
			yield self.env.timeout(1)


	def check_ingest_capacity(self, pipeline_demand):
		"""
		Check if the Cluster has the machine capacity to process the
		observation Ingest pipeline

		Observation objects have an observation type - this corresponds to
		an ingest pipeline that is set out in the Telescope. This pipeline type
		determines the number of machines in the cluster, and the duration,
		which must be reserved for the observation.

		Parameters
		----------
		pipeline_demand :  int
			The number of

		Returns
		-------
			True if the cluster has capacity
			False if the cluster does not have capacity to run the pipeline
		"""

		# Pipeline demand is the number of machines required for the pipeline
		# Length is how long the pipeline will take to
		# ingest/observation will take
		if len(self.available_resources) >= pipeline_demand:
			return True
		else:
			return False

	def provision_ingest_resources(self, demand, duration):
		"""
		Based on the requirements of the pipeline, provision a certain
		number of resources

		Parameters
		----------

		demand : int
			The type of ingest pipeline - see Observation
		duration : int

		Returns
		-------
		"""

		tasks = self._generate_ingest_tasks(demand, duration)
		# Generate machine/task pairs
		pairs = []
		resources = self.available_resources[:demand]
		self.available_resources = self.available_resources[demand:]
		self.ingest_resources = resources

		for x in range(demand):
			machine = resources[x]
			pairs.append((machine, tasks[x]))

		self.ingest = True
		while True:
			for pair in pairs:
				(machine, task) = pair
				if task not in self.running_tasks:
					self.running_tasks.append(task)
					self.env.process(machine.run(task))
					# status = machine.current_task.task_status
					# task.task_status=val
				else:
					break
			# yield self.env.timeout(1)
			yield self.env.timeout(1)
			for pair in pairs:
				(machine, task) = pair
				if task.task_status is TaskStatus.FINISHED:
					self.running_tasks.remove(task)
					self.finished_tasks.append(task)
					self.available_resources.append(machine)
					self.ingest_resources.remove(machine)
					continue
			if len(self.ingest_resources) == 0:
				# We've finished ingest
				break



		# return True


	# Mark resources as 'in-use' for the given pipeline.
	# Runs the task on the machie
	# Create a 'dummy' task for each machine, of the duration of the
	# Observation

	# task.length = length
	# self.cluster.allocate_task(task, machine)
	# if task.task_status is TaskStatus.SCHEDULED:
	# 	self.cluster.running_tasks.append(task)

	def _generate_ingest_tasks(self, demand, duration):
		"""
		Parameters
		----------
		demand : int
			Number of machines that are provisioned for ingest
		duration : int
			Duration of observation (in simulation timesteps)

		Returns
		-------
		tasks : list()
			List of core.Planner.Task objects
		"""
		tasks = []
		for i in range(demand):
			t = Task(i, env=self.env)
			t.duration = duration
			t.task_status = TaskStatus.SCHEDULED
			tasks.append(t)
		return tasks

	def availability(self):
		""" Returns
		-------
		availability: what resources are available
		"""
		availability = None
		for machine in self.machines:
			if machine.current_task:
				availability += 1

		return availability

	def resource_use(self):
		"""Returns the utilisation of the Cluster"""
		ustilisation = None
		for machine in self.machines:
			if machine.current_task:
				ustilisation += 1

		return ustilisation

	# TODO Place holder method
	def efficiency(self):
		"""

		Returns
		-------
		efficiency: The efficiency of the cluster
		"""
		efficiency = None
		for machine in self.machines:
			if machine.current_task:
				efficiency += 1
		return efficiency
