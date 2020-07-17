import json
from core.machine import Machine
from queue import Queue, PriorityQueue


class Cluster(object):
	def __init__(self, machine_config):
		self.machines = _process_machine_config(machine_config)
		self.running_tasks = []
		self.finished_tasks = []
		self.waiting_tasks = []
		# self.workflows = []  # Keeping here to stop runtime errors
		self.finished_workflows = []

	def has_capacity(self):
		if self.machines:
			return True
		else:
			return False

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


# Helper function that acts as static function for Cluster
def _process_machine_config(machine_config):
	with open(machine_config, 'r') as infile:
		config = json.load(infile)
	machines = config['system']['resources']
	machine_list = []
	for machine in machines:
		machine_list.append(Machine(
			machine,
			machines[machine]['flops'],
			1,
			1,
		))
	return machine_list
