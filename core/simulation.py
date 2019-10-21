from core.monitor import Monitor


class Simulation(object):
	def __init__(self, env, telescope, cluster, buffer, scheduler, event_file):
		self.env = env
		self.telescope = telescope
		self.cluster = cluster
		# self.task_broker = task_broker
		self.buffer = buffer
		self.scheduler = scheduler
		self.event_file = event_file
		if event_file is not None:
			self.monitor = Monitor(self)

		# self.task_broker.attach(self)
		self.scheduler.attach(self)
		self.telescope.attach(self)
		self.buffer.attach(self)

	def init_process(self):
		# Starting monitor process before task_broker process
		# and scheduler process is necessary for log records integrity.
		if self.event_file is not None:
			self.env.process(self.monitor.run())
		self.env.process(self.telescope.run())
		# self.env.process(self.buffer.run())
		# self.env.process(self.task_broker.run())
		self.env.process(self.scheduler.run())

	@property
	def finished(self):
		return not (
				self.telescope.observations or self.buffer.observations_for_processing
				or self.cluster.workflows
		)
