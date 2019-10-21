# from core.machine import Machine


class Cluster(object):
	def __init__(self):
		self.machines = []
		self.workflows = []
		self.workflow_plans = []
		self.queue = []

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

	def add_machines(self, machine_list):
		for machine in machine_list:
			self.machines.append(machine)
			machine.attach(self)

	def add_workflow(self,workflow_id):
		if workflow_id not in self.workflows:
			self.workflows.append(workflow_id)
			return True
		else:
			return False

	def add_workflow_plan(self, workflow_plan):
		if self.add_workflow(workflow_plan.id):
			self.workflow_plans.append(workflow_plan)
		else:
			print("Workflow for observation {0} is already waiting to be scheduled".format(workflow_plan.id))

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
			'arrived_jobs': len(self.workflows),
			# 'unfinished_jobs': len(self.unfinished_jobs),
			# 'unfinished_job_ids': str(self.unfinished_jobs),
			# 'finished_jobs': len(self.finished_jobs),
			# 'finished_job_ids': str(self.finished_jobs),
			# 'unfinished_tasks': len(self.unfinished_tasks),
			# 'finished_tasks': len(self.finished_tasks),
			# 'running_task_instances': len(self.running_task_instances),
			'machine_states': [machine.state for machine in self.machines],
			'observation_workflows_waiting': [plan.id for plan in self.workflow_plans],
			'cpu': self.cpu / self.cpu_capacity,
			'memory': self.memory / self.memory_capacity,
			'disk': self.disk / self.disk_capacity,
		}
