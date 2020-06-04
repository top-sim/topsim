import pandas as pd
import json

from core.monitor import Monitor
from core.scheduler import Scheduler
from core.cluster import Cluster
from core.visualiser import Visualiser
from core.telescope import Telescope, Observation
from core.buffer import Buffer
from core.planner import Planner

"""
Ideally, this is the 'final' class/object that we have tests for; everything should be working 
for it to be put in here, but as we are retroactively adding unit tests, this will evolve with
the rest of the unittests. 
"""
import logging
logger = logging.getLogger(__name__)


class Simulation(object):

	"""
	The Simulation class is a wrapper for all Actors; we start the simulation
	through the simulation class, which in turn invokes the initial Actors and
	monitoring, and provides the conditions for checking if the simulation has
	finished.
	"""

	def __init__(self, env, telescope_config, telescopemax, machine_config,salgorithm,palgorithm, event_file, visualisation=False):
		"""
		:param env:
		:param telescope_config:
		:param machine_config:
		:param salgorithm:
		:param palgorithm:
		:param event_file:
		"""

		self.env = env
		# Event file setup
		self.event_file = event_file
		self.visualisation = visualisation
		if event_file is not None:
			self.monitor = Monitor(self)
		if visualisation:
			self.visualiser = Visualiser(self)
		# Process necessary config files
		observations = _process_telescope_config(telescope_config)

		# Initiaise Actor and Resource objects
		self.cluster = Cluster(machine_config)
		self.buffer = Buffer(env, self.cluster)
		self.planner = Planner(env, palgorithm, machine_config)
		self.telescope = Telescope(env, observations, self.buffer, telescopemax, self.planner)
		self.scheduler = Scheduler(env, salgorithm, self.buffer, self.cluster, self.telescope)

	def start(self, runtime=150):
		# Starting monitor process before task_broker process
		# and algorithms process is necessary for log records integrity.
		if self.event_file is not None:
			self.env.process(self.monitor.run())
		if self.visualisation:
			self.env.process(self.visualiser.run())
		self.env.process(self.telescope.run())
		self.env.process(self.scheduler.run())
		# Calling env.run() invokes the processes passed in init_process()
		if runtime > 0:
			self.env.run(until=runtime)
		else:
			if not self.is_finished():
				self.env.run()

		logger.info("Simulation Finished @ %s", self.env.now)

	def is_finished(self):
		status = (
				not self.telescope.check_observation_status()
				and self.buffer.observations_for_processing.empty()
				and len(self.scheduler.waiting_observations) == 0
				and len(self.cluster.running_tasks) == 0
		)
		if status:
			# Using compound 'or' doesn't give us a True/False
			return True
		else:
			return False


def _process_telescope_config(telescope_config):
	observations = []
	infile = open(telescope_config)
	config = pd.read_csv(infile)
	# config.
	# Format is name, start, duration, demand, filename
	for i in range(len(config)):
		obs = config.iloc[i, :]
		observation = Observation(
			obs['name'],
			int(obs['start']),
			int(obs['duration']),
			int(obs['demand']),
			obs['filename']
		)
		observations.append(observation)
	infile.close()
	return observations




def _process_workflow_config(workflow):
	pass