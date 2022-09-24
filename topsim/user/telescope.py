# import simpy
# from core.planner import Planner
# import config_data
import pandas as pd
import logging

from topsim.core.instrument import Instrument, RunStatus
from topsim.core.scheduler import ScheduleStatus

LOGGER = logging.getLogger(__name__)


class TelescopeQueue:

    def __init__(self):
        self._queue = []

    def push(self, x):
        self._queue.append(x)

    def pop(self):
        return self._queue.pop(0)

    def size(self):
        return len(self._queue)

    def empty(self):
        return len(self._queue) == 0


class Telescope(Instrument):
    """
    The Telescope is a high-level abstraction of the Telescope and Telesopce
    Operations Centre/Monitor. It coordinates between the Scheduler to
    determine if the Cluster and the Buffer have capacity.

    Parameters
    ----------
    env : Simpy.Environment object
        The simulation environment

    config : core.config.Config object
        Config object with stored simulation configuration

    planner :  core.planner.Planner object
        The Planner actor for the current simulation

    scheduler : core.scheduler.Scheduler object
        The Scheduler actor for the current simulation

    Raises
    ------
    OSError
        This will be raised if we cannot read the Telescope config file.
    JSONDecodeError
        This will be raised if the config is not parseable JSON
    """

    name = 'telescope'

    def __init__(self, env, config, planner, scheduler):
        super().__init__(env, config, planner, scheduler)
        self.env = env
        try:
            (total_arrays, pipelines, observations,
             max_ingest) = config.parse_instrument_config(Telescope.name)
        except OSError:
            raise
        #: int: Total number of arrays used to observe
        self.total_arrays = total_arrays
        #:  `dict` of different `observation: pipeline` pairs
        self.pipelines = pipelines
        self.observations = observations
        self.max_ingest = max_ingest

        #: :py:obj:`~topsim.core.scheduler.Scheduler` object of Simulation
        self.scheduler = scheduler
        #: :py:obj:`~topsim.core.olanner.Planner` object of Simulation
        self.planner = planner
        self.observation_types = None
        self.events = []
        self.telescope_status = False
        self.telescope_use = 0
        self.delayed = False

    def run(self):
        """
        The entry point for the Telescope actor; this will make decisions per
        timestep and then once these decisions have been resolved, yield a
        timeout to indicate a single timestep has passed for the Telescope.

        Decisions made within this function are:

            * Check telescope capacity for running new observations

            * Communicate with Scheduler to determine if the Buffer and Cluster
            have capacity to run a new Observation (Ingest and Storage
            conditions).

            * Initiate the generation of a WorkflowPlan using the Planner, once
            and observation is scheduled for observation.

            * Finalise an Observation and initiate the 'clean-up'.

        Notes
        -----
        The procedure here follows an 'observation-led' approach to
        determining if the observation is able to run (hence
        `observation.is_ready()`. The idea is the
        observation is standard across any implementation of Instrument,
        so it's easier for user-definied instruments to defer to the
        observation to determine if it is possible to run the observation (
        i.e. have the observation check if it's Start/End time conditions
        have been met).

        Returns
        -------

        Yields
        ------
        self.env.timeout(1)
            A single simulation timestep
        """
        while self.has_observations_to_process():
            # Check if scheduler is delayed
            self.events = []
            if (
                    self.scheduler.schedule_status is ScheduleStatus.DELAYED
                    and not self.delayed):
                self.delayed = True

            for observation in self.observations:
                capacity = self.total_arrays - self.telescope_use
                # IF there is an observation ready for start
                if observation.is_ready(self.env.now, capacity):
                    LOGGER.info('Observation %s scheduled for %s',
                                observation.name, self.env.now)
                    # Observation is ready - is the Buffer/Cluster?
                    if self.scheduler.check_ingest_capacity(observation,
                                                            self.pipelines,
                                                            self.max_ingest):
                        ret = self.begin_observation(observation)
                        observation.ast = self.env.now

                        LOGGER.info('telescope is now using %s arrays',
                                    self.telescope_use)
                        process = self.env.process(
                            self.scheduler.allocate_ingest(observation,
                                                           self.pipelines,
                                                           self.planner))
                        self._add_event(observation, "telescope", "started")

                elif observation.is_finished(self.env.now,
                                             self.telescope_status):
                    observation.status = self.finish_observation(observation)
                    self._add_event(observation, "telescope", "finished")
                    LOGGER.info('Telescope is now using %s arrays',
                                self.telescope_use)
                else:
                    continue

            yield self.env.timeout(1)

        self.events = []

    def begin_observation(self, observation):
        """
        Update the telescope use status based on observation demand for antennas

        Parameters
        ----------
        observation : :py:obj:`~topsim.core.telescope.Observation`
            Observation that is chosen for allocation to the telescope

        Returns
        -------
            RunStatus.RUNNING

        """
        # TODO We do not actually do any sanity checking here to make sure
        #  the telescope use isn't greater than the total_capacity.

        self.telescope_use += observation.demand
        self.telescope_status = True
        return RunStatus.RUNNING

    def finish_observation(self, observation):
        """
        Update the telescope use status based on observation demand for antennas

        Parameters
        ----------
        observation : :py:obj:`~topsim.core.telescope.Observation`
            Observation that is chosen for allocation to the telescope

        Returns
        -------
            RunStatus.RUNNING

        """

        # TODO We do not actually do any sanity checking here to make sure
        #  the telescope use isn't greater than the total_capacity.

        self.telescope_use -= observation.demand

        if self.telescope_use == 0:
            self.telescope_status = False
        return RunStatus.FINISHED

    def run_observation_on_telescope(self, demand):
        pass

    def has_observations_to_process(self):
        for observation in self.observations:
            if observation.status == RunStatus.FINISHED:
                continue
            else:
                return True
        return False

    def make_greedy_decision(self):
        """
        This method acts as a 'greedy' decision maker, with the following
        policies:

            * If there is space in the buffer and cluster, and there is
            enough room on the telescope, schedule the observation
            * If there is a delay, drop the lowest priority observation from
            the plan.
                * If all observations are of the same priority, drop the
                shortest
                * If all observations are of the same length, drop the one
                with the highest data requirement
        Returns
        -------

        """

    def make_delay_conscious_decision(self):
        """
        This method takes into account delays on the telescope and acts to
        improve the science output based on the delays
        This will change based on whether we are in a Global or Independent
        DAG model.

        Independent DAG model - drop things

        Global DAG model - we reschedule based on current awareness of the
        cluster and the delays (with a buffer).
            This means we update our global plan, with the workflows
            potentially being interleaved more effectively as a result of the
            delay.

        Then, based on this re-planning, we determine the delay %; if it's still
        over, we follow the greedy approach

        Returns
        -------

        """

    def observations_waiting(self):
        """
        Calculate the number of observations that are waiting for telescope time
        Returns
        -------

        """
        return sum([1 if x.status == RunStatus.WAITING else 0 for x in
                    self.observations])

    def observations_finished(self):
        """
        Calculates the number of observations that are finished observing

        Returns
        -------
        Integer number of finished observations
        """
        return sum([1 if x.status == RunStatus.FINISHED else 0 for x in
                    self.observations])

    def print_state(self):
        return {'telescope_in_use': self.telescope_status,
                'telescope_arrays_used': self.telescope_use,
                'observations_waiting': self.observations_waiting()}

    def is_idle(self):
        """
        Check if telescope has current or pending observations

        Returns
        -------
        True if there are no pending or current observations
        """
        for observation in self.observations:
            if observation.status != RunStatus.FINISHED:
                return False
        if ((not self.telescope_status) and self.telescope_use == 0):
            return True
        return False

    def to_df(self):
        df = pd.DataFrame()
        df['observations_waiting'] = [self.observations_waiting()]
        df['observations_finished'] = [self.observations_finished()]
        df['observations_delayed'] = [self._calc_observation_delay()]
        return df

    def _calc_observation_delay(self):
        cum_delay = 0
        for observation in self.observations:
            if self.env.now > observation.est and (
                    observation.status == RunStatus.WAITING):
                cum_delay += self.env.now - observation.est
        return cum_delay

    def _add_event(self, observation, resource, event):
        self.events.append(
            {
                "time": int(self.env.now), "actor": "instrument",
                "observation": observation.name, "event": event,
                "resource": resource
            }
        )
