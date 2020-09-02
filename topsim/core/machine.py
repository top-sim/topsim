from enum import Enum

# TODO need I/O information here
from topsim.core.planner import TaskStatus


class Status(Enum):
	IDLE = 0
	IN_USE = 1
	RESERVED = 2
	ERROR = 3


class Machine(object):
	def __init__(self, id, cpu, memory, disk, bandwidth):
		self.id = id
		self.cpu = cpu
		self.memory = memory
		self.disk = disk
		self.bandwidth = bandwidth
		self.status = Status.IDLE
		self.transfer_flag = False
		self.current_task = None

	def run(self, task):
		# return True
		while True:
			self.run_task(task)
			if task.task_status is TaskStatus.SCHEDULED:
				run_status = task.run()
			if task.task_status is TaskStatus.RUNNING:
				yield task.env.timeout(1)
			if task.task_status is TaskStatus.FINISHED:
				self.stop_task(task)
				break

	def run_task(self, task_instance):
		self.cpu -= task_instance.flops
		self.memory -= task_instance.memory
		self.disk -= task_instance.io
		self.current_task = task_instance
		# self.task_instances.append(task_instance)
		self.status = Status.IN_USE

	def stop_task(self, task_instance):
		self.cpu += task_instance.flops
		self.memory += task_instance.memory
		self.disk += task_instance.io
		self.status = Status.IDLE
		self.current_task = None

	def transfer_data_from(self, machine):
		"""
		Transfer the data from :param: machine
		Parameters
		----------
		machine

		Returns
		-------

		"""
		pass

	def transfer_data_to(self, machine):
		pass

	def accommodate(self, task):
		return self.cpu >= task.task_config.flops and \
			   self.memory >= task.task_config.memory and \
			   self.disk >= task.task_config.io

	def state_summary(self):
		return {
			'id': self.id,
			'cpu': self.cpu / self.cpu_capacity,
			'memory': self.memory / self.memory_capacity,
			'disk': self.disk / self.disk_capacity,
		}

	def __eq__(self, other):
		return isinstance(other, Machine) and other.id == self.id

	def __repr__(self):
		return str(self.id)

	def print_state(self):
		return {
			'id': self.id,
			'cpu': self.cpu,
			'memory': self.memory,
			'disk': self.disk,
		}


def utilisation_policy(machine):
	return None
