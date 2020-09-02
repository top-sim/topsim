from topsim.core.monitor import Monitor
from topsim.core.scheduler import Scheduler
from topsim.core.cluster import Cluster
from topsim.core.visualiser import Visualiser
from topsim.core.telescope import Telescope
from topsim.core.buffer import Buffer
from topsim.core.planner import Planner

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

	Parameters
	----------


	Methods
	-------


	Raises
	------
	"""

	def __init__(
			self,
			env,
			telescope_config,
			cluster_config,
			buffer_config,
			planning_algorithm,
			scheduling_algorithm,
			event_file,
			visualisation=False
	):

		self.env = env
		# Event file setup
		self.event_file = event_file
		self.visualisation = visualisation
		if event_file is not None:
			self.monitor = Monitor(self)
		if visualisation:
			self.visualiser = Visualiser(self)
		# Process necessary config files

		# Initiaise Actor and Resource objects

		self.cluster = Cluster(env, cluster_config)
		self.buffer = Buffer(env, self.cluster, config=buffer_config)
		self.planner = Planner(env, planning_algorithm, cluster_config)
		self.scheduler = Scheduler(
			env, self.buffer, self.cluster, scheduling_algorithm
		)

		self.telescope = Telescope(
			env=self.env,
			config=telescope_config,
			planner=self.planner,
			scheduler=self.scheduler
		)

	def start(self, runtime=150):
		"""
		Run the simulation, either for the specified runtime, OR until the
		exit condition is reached:

			* There are no more observations to process,
			* There is nothing left in the Buffer
			* The Scheduler is not waiting to allocate machines to resources
			* There are not tasks still running on the cluster.


		Parameters
		----------
		runtime : int
			Nominiated runtime of the simulation. If the simulation length is
			known, pass that as the argument. If not, passing in a negative
			value (typically, just -1) will run the simulation until the
			exit condition is reached.

		Returns
		-------

		"""

		if self.event_file is not None:
			self.env.process(self.monitor.run())
		if self.visualisation:
			self.env.process(self.visualiser.run())

		self.env.process(self.telescope.run())
		self.env.process(self.cluster.run())

		self.scheduler.init()
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
				len(self.telescope.observations) == 0
				and self.buffer.observations_for_processing.empty()
				and len(self.scheduler.waiting_observations) == 0
				and len(self.cluster.running_tasks) == 0
		)
		if status:
			# Using compound 'or' doesn't give us a True/False
			return True
		else:
			return False


def _process_workflow_config(workflow):
	pass
