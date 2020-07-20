from enum import Enum

# TODO need I/O information here


class Status(Enum):
	IDLE = 0
	IN_USE = 1
	ERROR = 2


class Machine(object):
	def __init__(self, mid, cpu_capacity, memory_capacity, disk_capacity, cpu=None, memory=None, disk=None):
		self.id = mid
		self.cpu_capacity = cpu_capacity
		self.memory_capacity = memory_capacity
		self.disk_capacity = disk_capacity
		self.cpu = cpu_capacity if cpu is None else cpu
		self.memory = memory_capacity if memory is None else memory
		self.disk = disk_capacity if disk is None else disk
		self.status = Status.IDLE
		self.transfer_flag = False
		self.current_task = None

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
			'cpu_capacity': self.cpu_capacity,
			'memory_capacity': self.memory_capacity,
			'disk_capacity': self.disk_capacity,
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