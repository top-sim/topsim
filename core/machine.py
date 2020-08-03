from enum import Enum

# TODO need I/O information here
from core.planner import TaskStatus


class Status(Enum):
	IDLE = 0
	IN_USE = 1
	RESERVED = 2
	ERROR = 3


class Machine(object):
	def __init__(self, id, cpu, memory, disk,bandwidth):
		self.id = id
		self.cpu = cpu
		self.memory = memory
		self.disk = disk
		self.bandwidth = bandwidth
		self.status = Status.IDLE
		self.transfer_flag = False
		self.current_task = None

	def run(self, task):
		if task.task_status is TaskStatus.SCHEDULED:
			self.run_task(task)
		else:
			return False
		run_status = task.run()
		if run_status is TaskStatus.FINISHED:
			self.stop_task(task)
			return True
		else:
			raise RuntimeError(
				'Machine: {0} failed to execute Task: {1}'.format(self, task)
			)

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


def utilisation_policy(machine):
	return None