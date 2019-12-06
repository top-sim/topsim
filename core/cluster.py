import json
from core.machine import Machine
from queue import Queue


class Cluster(object):
	def __init__(self, machines):
		self.machines = []
		self._add_machines(machines)
		# self.workflows = []  # Keeping here to stop runtime errors
		self.queue = Queue()
		self.finished_workflows = []

	def _add_machines(self, machine_config):
		machines = _process_machine_config(machine_config)
		for machine in machines:
			self.machines.append(machine)
			machine.attach(self)

	@property
	def unfinished_jobs(self):
		ls = []
		for job in self.workflows:
			if not job.finished:
				ls.append(job)
		return ls

	@property
	def unfinished_tasks(self):
		ls = []

		for job in self.workflows:

			ls.extend(job.unfinished_tasks)
		return ls

	@property
	def tasks_which_has_waiting_instance(self):
		ls = []
		for job in self.workflows:
			ls.extend(job.tasks_which_has_waiting_instance)
		return ls

	@property
	def finished_jobs(self):
		ls = []
		for job in self.workflows:
			if job.finished:
				ls.append(job)
		return ls

	@property
	def finished_tasks(self):
		ls = []
		for job in self.workflows:
			ls.extend(job.finished_tasks)
		return ls

	@property
	def running_task_instances(self):
		task_instances = []
		for machine in self.machines:
			task_instances.extend(machine.running_task_instances)
		return task_instances

	@property
	def cpu(self):
		return sum([machine.cpu for machine in self.machines])

	@property
	def memory(self):
		return sum([machine.memory for machine in self.machines])

	@property
	def disk(self):
		return sum([machine.disk for machine in self.machines])

	@property
	def cpu_capacity(self):
		return sum([machine.cpu_capacity for machine in self.machines])

	@property
	def memory_capacity(self):
		return sum([machine.memory_capacity for machine in self.machines])

	@property
	def disk_capacity(self):
		return sum([machine.disk_capacity for machine in self.machines])

	@property
	def state(self):
		return {
			'arrived_workflows': len(self.workflows),
			'unfinished_workflows': len(self.workflows),
			'finihsed_workflows': self.workflows,
			# 'unfinished_jobs': len(self.unfinished_jobs),
			# 'unfinished_job_ids': str(self.unfinished_jobs),
			# 'finished_jobs': len(self.finished_jobs),
			# 'finished_job_ids': str(self.finished_jobs),
			# 'unfinished_tasks': len(self.unfinished_tasks),
			# 'finished_tasks': len(self.finished_tasks),
			# 'running_task_instances': len(self.running_task_instances),
			'machine_states': [machine.state for machine in self.machines],
			# 'observation_workflows_waiting': [plan.id for plan in self.workflow_plans],
			'cpu': self.cpu / self.cpu_capacity,
			'memory': self.memory / self.memory_capacity,
			'disk': self.disk / self.disk_capacity,
		}


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
	pass

