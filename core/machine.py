from enum import Enum


# class MachineConfig(object):
#     idx = 0
#
#     def __init__(self, cpu_capacity, memory_capacity, disk_capacity, cpu=None, memory=None, disk=None):
#         self.cpu_capacity = cpu_capacity
#         self.memory_capacity = memory_capacity
#         self.disk_capacity = disk_capacity
#
#         self.cpu = cpu_capacity if cpu is None else cpu
#         self.memory = memory_capacity if memory is None else memory
#         self.disk = disk_capacity if disk is None else disk
#
#         self.id = MachineConfig.idx
#         MachineConfig.idx += 1


class MachineDoor(Enum):
	TASK_IN = 0
	TASK_OUT = 1
	NULL = 3


class Machine(object):
	def __init__(self, mid, cpu_capacity, memory_capacity, disk_capacity, cpu=None, memory=None, disk=None):
		self.id = mid
		self.cpu_capacity = cpu_capacity
		self.memory_capacity = memory_capacity
		self.disk_capacity = disk_capacity
		self.cpu = cpu_capacity if cpu is None else cpu
		self.memory = memory_capacity if memory is None else memory
		self.disk = disk_capacity if disk is None else disk
		self.machine_door = MachineDoor.NULL
		self.current_task = None

	def run_task(self, task_instance):
		self.cpu -= task_instance.flops
		self.memory -= task_instance.memory
		self.disk -= task_instance.io
		self.current_task = task_instance
		# self.task_instances.append(task_instance)
		self.machine_door = MachineDoor.TASK_IN

	def stop_task(self, task_instance):
		self.cpu += task_instance.flops
		self.memory += task_instance.memory
		self.disk += task_instance.io
		self.machine_door = MachineDoor.TASK_OUT
		self.current_task = None

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
