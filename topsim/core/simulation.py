"""
The Simulation class is a wrapper for all Actors; we start the simulation
through the simulation class, which in turn invokes the initial Actors and
monitoring, and provides the conditions for checking if the simulation has
finished.
"""

import logging
import time
import json
from topsim.core.config import Config
from topsim.core.monitor import Monitor
from topsim.core.scheduler import Scheduler
from topsim.core.cluster import Cluster
from topsim.core.buffer import Buffer
from topsim.core.planner import Planner
from topsim.core.delay import DelayModel

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

    telescope_config: `str`
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
        instance of the ``core.algorithm`` interface

    sim_timestamp: str
        Optional Simulation start-time; this is useful for testing, to ensure we
        name the file and the tests match up. Also useful if you do not want to
        use the time of the simulation as the name.

    visualisation: bool
        If visualisation is required, True; else, False


    Notes
    -----
    An Index instance can **only** contain hashable objects

    Examples
    --------

    Standard simulation with data frame output

    >>> env = simpy.environment()
    >>> config = Config('path/to/config')
    >>> instrument = CustomInstrument()
    >>> plan = PlanningModel()
    >>> sched = SchedulingModel()
    >>> simulation = Simulation(env, config, instrument,plan,sched)

    If we want delays in the model:

    >>> dm = DelayModel(prob=0.1, dist='normal', dm=DelayModel.DelayDegree.LOW)
    >>> simulation =  Simulation(
    >>>    env, config, instrument,plan,sched, delay=dm
    >>> )


    Raises
    ------
    """

    def __init__(
            self,
            env,
            config,
            instrument,
            planning_model,
            planning_algorithm,
            scheduling,
            delay=None,
            timestamp=None,
            to_file=False,
    ):

        self.env = env

        if timestamp:
            self.monitor = Monitor(self, timestamp)
        else:
            sim_start_time = f'{time.time()}'.split('.')[0]
            self.monitor = Monitor(self, sim_start_time)
        # Process necessary config files

        #: Configuration path
        self.cfgpath = config
        # Initiaise Actor and Resource objects
        cfg = Config(config)
        self.cluster = Cluster(env, cfg)
        self.buffer = Buffer(env, self.cluster, cfg)
        planning_algorithm = planning_algorithm
        planning_model = planning_model

        if not delay:
            delay = DelayModel(0.0, "normal", DelayModel.DelayDegree.NONE)
        self.planner = Planner(
            env, planning_algorithm, self.cluster, planning_model, delay
        )
        scheduling_algorithm = scheduling()
        self.scheduler = Scheduler(
            env, self.buffer, self.cluster, scheduling_algorithm
        )
        self.instrument = instrument(
            env=self.env,
            config=cfg,
            planner=self.planner,
            scheduler=self.scheduler
        )

        self.to_file = to_file

        self.running = False

    def start(self, runtime=-1):
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
        self.env.process(self.monitor.run())
        self.env.process(self.instrument.run())
        self.env.process(self.cluster.run())

        self.scheduler.start()
        self.env.process(self.scheduler.run())
        self.env.process(self.buffer.run())

        if runtime > 0:
            self.env.run(until=runtime)
        else:
            while not self.is_finished():
                self.env.run(self.env.now + 1)
            self.env.run(self.env.now + 1)
        LOGGER.info("Simulation Finished @ %s", self.env.now)

        if self.to_file:
            self.monitor.df.to_pickle(f'{self.monitor.sim_timestamp}-sim.pkl')
            self._generate_final_task_data().to_pickle(
                f'{self.monitor.sim_timestamp}-tasks.pkl'
            )
        else:
            return self.monitor.df, self._generate_final_task_data()

    def resume(self, until):
        """
        Resume a simulation for a period of time
        Useful for testing purposes, as we do not re-initialise the process
        calls as we used to in :meth:`core.topsim.simulation.Simulation.start`

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
        if (
                self.buffer.is_empty() and self.cluster.is_idle() and
                self.scheduler.is_idle() and self.instrument.is_idle()
        ):
            return True
        return False

    @staticmethod
    def _split_monolithic_config(self, json):
        return json

    def _generate_final_task_data(self):
        """
        Generate a final data frame from the cluster's task dataframe output.
        Returns
        -------

        """

        df = self.cluster.finished_task_time_data()
        df = df.T
        size = len(df)
        df['scheduling'] = [self.planner.algorithm for x in range(size)]
        df['planning'] = [
            repr(self.scheduler.algorithm) for x in range(size)
        ]
        df['config'] = [self.cfgpath for x in range(size)]
        return df
