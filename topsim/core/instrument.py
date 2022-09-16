# Copyright (C) 28/1/21 RW Bunney

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from abc import ABC, abstractmethod
from enum import Enum


class Instrument(ABC):
    """

    The Simulation class is a wrapper for all Actors; we start the simulation
    through the simulation class, which in turn invokes the initial Actors and
    monitoring, and provides the conditions for checking if the simulation has
    finished.

    Parameters
    ----------
    env : :py:obj:`simpy.Environment` object
        The discrete-event simulation environment. This is the way TOpSim
        simulation maintains state across the different actors,
        and interfaces with the simpy processes.

    config : :py:obj:`~topsim.core.config.Config` instance
        Config object wrapper for the configuration file.

    planner : :py:obj:`~topsim.core.planner.Planner` instance
        The Planner Actor for the current simulation

    scheduler : :py:obj:`~topsim.core.scheduler.Scheduler` instance
        The Scheduler Actor for the current simulation

    Notes
    -----

    This class is a Python :py:obj:`~abc.ABC`, meaning it requires user
    addition to implement the metaclasses.

    Recommended decisions to make in the `run()` method include:

    Check observation and instrument are ready given current instrument
    demand:

    >>> # Assuming self.capacity is a user-defined attribute
    >>> if(observation.is_ready(self.env.now, self.capacity))

    Communicate with Scheduler to determine if the Buffer and Cluster have
    capacity to run a new Observation (Ingest and Storage conditions):

    >>> self.scheduler.check_ingest_capacity(
    >>>    observation, pipelines, max_ingest
    >>>)

    If above conditions are reached, begin observations and request ingest
    allocation via scheduler:

    >>> self.begin_observation(observation)
    >>> self.env.process(self.scheduler.allocate_ingest(
    >>> observation, pipelines, planner))

    **Note:** :py:meth:`~topsim.core.scheduler.Scheduler.allocate_ingest`
    generates a timeout on the `SimPy` discrete-event queue, which is why
    we call `env.process`.

    Finalise an Observation and initiate the 'clean-up'.

    >>> observation.status = self.finish_observation(observation)


    See Also
    ---------
    :py:obj:`~topsim.user.telescope.Telescope`

    Raises
    ------

    """

    def __init__(self, env, config, planner, scheduler):
        pass

    @abstractmethod
    def run(self):
        """
        The starting point for the Instrument actor; this will make
        decisions per timestep and then once these decisions have been
        resolved, yield a timeout to indicate a single timestep has passed
        for the Telescope.

        Yields
        ------
        self.env.timeout(x)
            A single simulation timestep of `x` time.
        """

        pass

    @abstractmethod
    def to_df(self):
        """
        Produce a `pandas.DataFrame` of output for the Simulation
        :py:obj:`~topsim.core.monitor.Monitor`.
        Returns
        -------

        """
        pass


class Observation(object):
    """
    Observation object stores information about a given observation;
    the object also stores information about the workflow, and the generated
    plan for that workflow.

    Parameters
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

    def __init__(self, name, start, duration, demand, workflow,
                 data_rate, timestep='seconds',min_resources=-1,max_resources=-1):

        # TODO change to self.id
        self.name = name
        self.buffer_id = 0
        self.cluster_id = 'default'
        # self.start = start
        self.est = start
        self.ast = None
        self.duration = duration
        self.demand = demand
        self.workflow = workflow
        self.total_data_size = 0
        self.ingest_data_rate = data_rate
        self.timestep = timestep
        self.status = RunStatus.WAITING
        self.min_resources = min_resources
        self.min_resources = max_resources

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
        if self.est <= current_time \
                and self.demand <= capacity \
                and self.status is RunStatus.WAITING:
            return True
        else:
            return False

    def is_finished(self, current_time, telescope_status):
        """
        Check if the observation has finished on the telescope.
        Parameters
        ----------
        current_time
        telescope_status

        Returns
        -------

        """
        if self.ast is None:
            return False
        elif current_time >= self.ast + self.duration \
                and telescope_status \
                and (self.status is not RunStatus.FINISHED):
            return True
        else:
            return False


class RunStatus(str, Enum):
    """
    The status of an observation
    """
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
