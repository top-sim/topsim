class Scheduler(object):
	def __init__(self, env, algorithm):
		self.env = env
		self.algorithm = algorithm
		self.simulation = None
		self.cluster = None
		self.destroyed = False
		self.workflow_plans = []
		self.valid_pairs = {}

	def attach(self, simulation):
		self.simulation = simulation
		self.cluster = simulation.cluster

	def _init_workflow(self, workflow):
		# Get the nodes for the workflow from somewhere
		pass

	def make_decision(self):
		print(self.cluster.workflows)
		if self.workflow_plans:
			print("Scheduler: Waiting to process", self.workflow_plans)
		else:
			print("Scheduler: Nothing to process")
		max = -1
		current_plan = None
		for plan in self.cluster.workflow_plans:
			if max == -1 or plan.start_time < max:
				max = plan.start_time
				current_plan = plan
		if current_plan:
			print(current_plan.id, current_plan.exec_order)
			while True:
				machine, task = self.algorithm(self.cluster, self.env.now)
				if machine is None or task is None:
					break
				else:
					self.allocate_task(task, machine)
					# task.start_task_instance(machine)
		# while True:
		# 	machine, task = self.algorithm(self.cluster, self.env.now)
		# 	if machine is None or task is None:
		# 		break
		# 	else:
		# 		task.start_task_instance(machine)

	def allocate_task(self, task, machine):
		"""
		We are going to move away slightly from the 'task initiates its allocation', which was
		the method elected in CloudSimPy; instead, the scheduler performs the allocation (I prefer
		this from a logical/Actor way of thinking').
		:param task:
		:param machine:
		:return:
		"""

		pass

	def run(self):
		while not self.simulation.finished:
			self.make_decision()
			yield self.env.timeout(1)
	#
	# def add_workflow(self, workflow):
	# 	print("Adding", workflow, "to workflows")
	# 	self.observations_for_processing.append(workflow)
	# 	print("Waiting workflows", self.observations_for_processing)

	@property
	def state(self):
		# Change this to 'workflows scheduled/workflows unscheduled'
		return {
			# 'observations_for_processing': [plan.id for plan in self.workflow_plans]
		}
