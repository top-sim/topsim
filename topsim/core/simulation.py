"""
The Simulation class is a wrapper for all Actors; we start the simulation
through the simulation class, which in turn invokes the initial Actors and
monitoring, and provides the conditions for checking if the simulation has
finished.
"""


import logging
from topsim.core.monitor import Monitor
from topsim.core.scheduler import Scheduler
from topsim.core.cluster import Cluster
from topsim.core.visualiser import Visualiser
from topsim.core.telescope import Telescope
from topsim.core.buffer import Buffer
from topsim.core.planner import Planner
from topsim.algorithms.scheduling import FifoAlgorithm

LOGGER = logging.getLogger(__name__)


class Simulation:
    """
    The Simulation class is a wrapper for all Actors; we start the simulation
    through the simulation class, which in turn invokes the initial Actors and
    monitoring, and provides the conditions for checking if the simulation has
    finished.

    Parameters
    ----------
    env : simpy.Environment bject
        This is how the TOpSim simulation maintains state across the
        different actors, and interfaces with the simpy processes.

    telescope_config: str
        This is a path to the telescope config that follows the TOpSim config
        specification (JSON). This file will be parsed in the Telescope class
        constructure

    cluster_config: str
        Path to the HPC cluster config that forms the computing component of
        the SDP

    buffer_config: str
        Path to the buffer configuration

    planning_algorithm: Object
        instance of the planning algorithm class interface as defined in
        algorithms.examples/

    scheduling_algorithm: object
        instance of the core.algorithm interface

    event_file: str
        Path to the output file that stsores execution of simulation.

    visualisation: bool
        If visualisation is required, True; else, False

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
        self.planner = Planner(env, planning_algorithm, self.cluster)
        self.scheduler = Scheduler(
            env, self.buffer, self.cluster, scheduling_algorithm
        )
        self.telescope = Telescope(
            env=self.env,
            config=telescope_config,
            planner=self.planner,
            scheduler=self.scheduler
        )

        self.running = False

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
        if self.running:
            raise RuntimeError(
                "start() has already been called!"
                "Use resume() to continue a simulation that is already in "
                "progress."
            )
        self.running = True
        if self.event_file is not None:
            self.env.process(self.monitor.run())
        if self.visualisation:
            self.env.process(self.visualiser.run())

        self.env.process(self.telescope.run())
        self.env.process(self.cluster.run())

        self.scheduler.init()
        self.env.process(self.scheduler.run())
        self.env.process(self.buffer.run())
        # Calling env.run() invokes the processes passed in init_process()
        if runtime > 0:
            self.env.run(until=runtime)
        else:
            if not self.is_finished():
                self.env.run()

        LOGGER.info("Simulation Finished @ %s", self.env.now)

    def resume(self, until):
        """
        Resume a simulation for a period of time
        Useful for testing purposes, as we do not re-initialise the process
        calls as we used to in Simulation.start()
        Parameters
        ----------
        until : int
            The (non-inclusive) Simpy.env.now timestep that we want to
            continue to in the simulation

        Returns
        -------
        self.env.now : float
            The current time in the simulation
        """
        if not self.running:
            raise RuntimeError(
                "Simulation has not been started! call start() to initialise "
                "the process stack."
            )
        self.env.run(until=until)


    def is_finished(self):
        return False