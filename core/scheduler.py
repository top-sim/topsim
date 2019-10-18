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
		# while True:
		# 	machine, task = self.algorithm(self.cluster, self.env.now)
		# 	if machine is None or task is None:
		# 		break
		# 	else:
		# 		task.start_task_instance(machine)

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
			'observations_for_processing': [plan.id for plan in self.workflow_plans]
		}
