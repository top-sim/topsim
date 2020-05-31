import json


class Monitor(object):
	def __init__(self, simulation):
		self.simulation = simulation
		self.env = simulation.env
		self.event_file = simulation.event_file
		self.events = []

	def run(self):
		while not self.simulation.is_finished():
			state = {
				'timestamp': self.env.now,
				# 'cluster_state': self.simulation.cluster.state,
				'telescope_state': self.simulation.telescope.print_state(),
				'scheduler_state': self.simulation.scheduler.print_state()
			}
			print("Storing state...", self.simulation.scheduler.print_state())
			self.events.append(state)
			yield self.env.timeout(1)
			self.write_to_file()

		state = {
			'timestamp': self.env.now,
			# 'cluster_state': self.simulation.cluster.state,
			'telescope_state': self.simulation.telescope.print_state(),
			'scheduler_state': self.simulation.scheduler.print_state()
		}
		self.events.append(state)

		self.write_to_file()

	def write_to_file(self):
		with open(self.event_file, 'w+') as f:
			json.dump(self.events, f, indent=4)
