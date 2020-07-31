import json
from core.machine import Machine
from core import config
from queue import Queue, PriorityQueue

from core.planner import TaskStatus


class Cluster(object):
	def __init__(self,env, machine_config):

		self.machines = config.process_machine_config(machine_config)
		self.dmachine = {machine.id:machine for machine in self.machines}
		self.running_tasks = []
		self.finished_tasks = []
		self.waiting_tasks = []
		# self.workflows = []  # Keeping here to stop runtime errors
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


	def ingest_pipeline_provision(self, observation, ingest_time):
		"""
		For a given observation (param), we  will take up a certain amount of the system resources
		for a set period of time for 'ingest' computation. This will likely be almost all the time, but the
		resources that are selected may change based on the observation (e.g. larger
		telescope demans = more resources)

		When coordinating with the buffer, we want to pass to the buffer the amount of data produced by
		the ingest pipeline every timesetp, which for now is the observation.data_product_size / ingest_time
		:param observation:
		:return:
		"""

		return True


	def has_capacity(self):
		"""
		:return:
		"""
		if self.machines:
			return True
		else:
			return False

	def provision_ingest_resources(self, pipeline):
		"""
		Based on the requirements of the pipeline, provision a certain number of resources
		:param pipeline: The type of ingest pipeline - see Observation
		:return: None
		"""
		# Mark resources as 'in-use' for the given pipeline.
		pass

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

