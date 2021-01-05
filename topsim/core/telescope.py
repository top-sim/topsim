# import simpy
# from core.planner import Planner
# import config_data
import pandas as pd
import logging
# CHANGE THIS TO GET DEBUG VALUES FROM LOGS
import json

from enum import Enum

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


class Telescope:
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

    Attributes
    ----------
    env :

    total_arrays :

    pipelines : dict
        dictionary of the different pipelines that may be observed with this
        telescope setup. pipelines have a name associated with them, and an
        associated cluster-demand, which is the number of machines used during
        execution of the INGEST pipeline

    observations : list

    scheduler : core.Scheduler object

    planner : core.Scheduler object

    observation_types = None

    telescope_status = False

    telescope_use = 0

    Raises
    ------
    OSError
        This will be raised if we cannot read the Telescope config file.
    JSONDecodeError
        This will be raised if the config is not parseable JSON
    """

    def __init__(
            self, env, config, planner, scheduler
    ):
        self.env = env
        try:
            (
                self.total_arrays,
                self.pipelines,
                self.observations,
                self.max_ingest
            ) = config.parse_telescope_config()
        except OSError:
            raise
        self.scheduler = scheduler
        self.observation_types = None
        self.telescope_status = False
        self.telescope_use = 0
        self.planner = planner

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

        Returns
        -------

        Yields
        ------
        self.env.timeout(1)
            A single simulation timestep
        """

        while self.has_observations_to_process():
            for observation in self.observations:
                capacity = self.total_arrays - self.telescope_use
                # IF there is an observation ready for start
                if observation.is_ready(self.env.now, capacity):
                    LOGGER.info(
                        'Observation %s scheduled for %s',
                        observation.name,
                        self.env.now
                    )
                    # Observation is ready - is the Buffer/Cluster?
                    if self.scheduler.check_ingest_capacity(
                            observation,
                            self.pipelines,
                            self.max_ingest
                    ):
                        ret = self.begin_observation(observation)
                        self.env.process(
                            self.planner.run(observation)
                        )
                        # yield plan_trigger
                        LOGGER.info(
                            'telescope is now using %s arrays',
                            self.telescope_use
                        )
                        process = self.env.process(
                            self.scheduler.allocate_ingest(
                                observation, self.pipelines
                            ))

                elif observation.is_finished(
                        self.env.now,
                        self.telescope_status
                ):
                    observation.status = self.finish_observation(observation)
                    LOGGER.info(
                        'Telescope is now using %s arrays', self.telescope_use
                    )
                else:
                    continue

            yield self.env.timeout(1)

    def begin_observation(self, observation):
        self.telescope_use += observation.demand
        self.telescope_status = True
        return RunStatus.RUNNING

    def finish_observation(self, observation):
        self.telescope_use -= observation.demand

        if self.telescope_use is 0:
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
            potentially being interleaved more effectively as a result of the delay.

        Then, based on this re-planning, we determine the delay %; if it's still
        over, we follow the greedy approach

        Returns
        -------

        """

    def observations_waiting(self):
        return sum(
            [1 if x.status is not RunStatus.FINISHED
             else 0
             for x in self.observations]
        )

    def observations_finished(self):
        return sum(
            [1 if x.status is RunStatus.FINISHED
             else 0
             for x in self.observations]
        )

    def print_state(self):
        return {
            'telescope_in_use': self.telescope_status,
            'telescope_arrays_used': self.telescope_use,
            'observations_waiting': self.observations_waiting()
        }

    def to_df(self):
        df = pd.DataFrame()
        df['observations_waiting'] = [self.observations_waiting()]
        df['observations_finished'] = [self.observations_finished()]
        df['telescope_status'] = [self.telescope_status]
        return df


class Observation(object):
    """
    Observation object stores information about a given observation;
    the object also stores information about the workflow, and the generated
    plan for that workflow.

    Array with associated photographic information.

       Parameters
       ----------
    name : str
        Observation name/ID
    start : int
        Expected start-time of the observation
    duration : int
        Expected Duration of the observation
    demand : int
        Expected Telescope demand of (Number of arrays used) during observation
    workflow : str
        Path to the workflow specification (JSON file)
    type : str
        What type of observation (Continuum, Spectral, etc.)
    data_rate: int
        Expected incoming data rate produced by the observation (GB/s)

    Attributes
    ----------
    name : str
        Observation name
    start : int
        Expected start-time of the observation
    duration : int
        Expected Duration of the observation
    demand : int
        Expected Telescope demand of (Number of arrays used) during observation
    workflow : str
        Path to the workflow specification (JSON file)
    type : str
        What type of observation (Continuum, Spectral, etc.)
    ingest_data_rate : int
        Expected incoming data rate produced by the observation (GB/s)
    total_data_size : int
        Total size of the data produced by the observation. This is updated
        every simulation time step based on the duration of the observation
        and ingest_data_rate
    plan : WorkflowPlan object
        Workflow pre-schedule generated by the Planner once observation has
        been started on Telescope.
    """

    def __init__(self, name, start, duration, demand, workflow, type,
                 data_rate):

        self.name = name
        self.start = start
        self.duration = duration
        self.demand = demand
        self.status = RunStatus.WAITING
        self.type = type
        self.workflow = workflow
        self.total_data_size = 0
        self.ingest_data_rate = data_rate
        self.plan = None

    def is_ready(self, current_time, capacity):
        """

        Parameters
        ----------
        current_time: int
            The current simulation time (Simpy.env.now)
        capacity : int
            Current capacity of the telescope at this time

        Returns
        -------
        True if telescope has capacity, and the observation is scheduled to
        start from now

        False if telescope does not have capacity

        """
        if self.start <= current_time \
                and self.demand <= capacity \
                and self.status is RunStatus.WAITING:
            return True
        else:
            return False

    def is_finished(self, current_time, telescope_status):
        if current_time >= self.start + self.duration \
                and telescope_status \
                and (self.status is not RunStatus.FINISHED):
            return True
        else:
            return False


def process_observation_template(config_dict):
    """
    Read in observation dictionary
    :return: Observation object

    """
    observation = config_dict

    return observation


def process_telescope_config(telescope_config):
    try:
        with open(telescope_config, 'r') as infile:
            config = json.load(infile)
    except OSError:
        LOGGER.warning("File %s not found", telescope_config)
        raise
    except json.JSONDecodeError:
        LOGGER.warning("Please check file is in JSON Format")
        raise
    try:
        'telescope' in config and 'observation' in config
    except KeyError:
        LOGGER.warning(
            "'telescope/observation' is not in %s, "
            "check your JSON is correctly formatted",
            config
        )
        raise
    total_arrays = config['telescope']['total_arrays']
    pipelines = config['telescope']['pipelines']
    observations = []
    for observation in config['telescope']['observations']:
        try:
            o = Observation(
                name=observation['name'],
                start=observation['start'],
                duration=observation['duration'],
                demand=observation['demand'],
                workflow=observation['workflow'],
                type=observation['type'],
                data_rate=observation['data_product_rate']
            )
            observations.append(o)
        except KeyError:
            raise
    max_ingest_resources = config['telescope']['max_ingest_resources']
    return total_arrays, pipelines, observations, max_ingest_resources


class RunStatus(str, Enum):
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
