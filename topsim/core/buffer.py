# Copyright (C) 10/19 RW Bunney

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

"""
buffer.py contains three main class that encapsulate the behaviour of the
Buffer actor in a simulation.

* Buffer : This is the high-level buffer that other actors interact with. It
is the one that is initalised at the beginning of a simulation, and who's
process is invoked with simpy.env.process.

* HotBuffer : This is the streaming buffer in which instrument ingest occurs,
and from which ingest processing pipelines may be run.

* ColdBuffer : This is the main storage buffer, where post-ingest data is
moved and from where post-processing pipelines access workflow data.
"""

import logging
import json
import pandas as pd

from topsim.common.globals import TIMESTEP
from topsim.core.telescope import RunStatus

LOGGER = logging.getLogger(__name__)


class Buffer:
    """
    Parameters
    ----------
    env : core.Simpy.Environment object
        The simulation environment

    Attributes
    ---------

    Methods
    -------
    """

    def __init__(self, env, cluster, config):
        """

        Parameters
        ----------
        env : simpy.Environment
            The environment object for the Simulation
        cluster : topsim.core.cluster.Cluster
            Cluster (Actor) object for the simulation
        config : str
            path to the Buffer JSON configuration file
        """
        self.env = env
        self.cluster = cluster
        # We are reading this from a file, check it works
        try:
            self.hot, self.cold = process_buffer_config(config)
        except OSError:
            print("Error processing Buffer config file")
            raise

        self.buffer_alloc = {}
        self.waiting_observation_list = []

    def run(self):
        """
        This is the main process of the Buffer to have it running continually
        during the simulation.

        At each timestep, the buffer checks if there is an observation stored
        on the HotBuffer, and schedules it to move to longer-term storage on
        the cold buffer, in order to make room for new ingest/observations.

        Yields
        -------
        A simpy.env.timeout() of duration topsim.common.globals.TIMESTEP
        """
        while True:
            LOGGER.info(
                "HotBuffer: %s \nColdBuffer: %s",
                self.hot.current_capacity,
                self.cold.current_capacity
            )
            if self.hot.has_waiting_observations():
                self.env.process(self.move_hot_to_cold())
            yield self.env.timeout(TIMESTEP)

    def check_buffer_capacity(self, observation):
        """
        Determines if there is capacity in both the Hot and Cold Buffers

        Parameters
        ----------
        observation : topsim.core.telescope.Observation
            The observation intended to be added to the buffer

        Returns
        -------
        True :
            If both buffers have capacity
        False :
            If at least one buffer does not have capacity
        """
        size = observation.ingest_data_rate * observation.duration
        if self.hot.current_capacity - size < 0 \
                or not self.cold.has_capacity(size):
            return False

        return True

    def ingest_data_dump(self, data):
        pass

    def has_observations_ready_for_processing(self):
        """
        Observations that are available for processing must come from the
        ColdBuffer.

        Returns
        -------
        True if has observations; False if not.
        """
        return self.cold.has_stored_observations()

    def next_observation_for_processing(self):
        """
        Calls the cold buffer and checks if there are any observations
        for processing

        Returns
        -------
        observation : topsim.core.telescope.Observation()
            next observation for processing
        """
        return self.cold.next_observation_for_processing()

    def mark_observation_finished(self, observation):
        """

        To be called by the Scheduler.

        This marks an observation as 'finished' in the buffer, so it is
        no longer accessible from the ColdBuffer storage.

        Parameters
        ----------
        observation : topsim.core.Telescope.Observation
            The observation that has been processed on the scheduler.

        Returns
        -------
        The result of ColdBuffer.remove(observation), which is a boolean value
        which is True if the observation is in the ColdBuffer, and false if it
        is not.
        """
        return self.cold.remove(observation)

    def move_hot_to_cold(self):
        """

        Called when the scheduler is requesting data for workflow processing.

        This method 'moves' the observation data from the HotBuffer to the
        ColdBuffer, at a rate of  ColdBuffer.max_data_rate.

        ----------
        observation : core.telescope.Observation object

            The observation that is stored in the HotBuffer, to be moved

        Returns
        -------

        """
        if not self.hot.observations["stored"]:
            raise RuntimeError(
                "No observations in Hot Buffer"
            )
        current_obs = self.hot.observation_for_transfer()
        # current_obs = self.hot.observations['transfer']
        data_left_to_transfer = current_obs.total_data_size
        if not self.cold.has_capacity(data_left_to_transfer):
            # We cannot actually transfer the observation due to size
            # constraints
            self.hot.observations['stored'].append(current_obs)
            self.hot.observations['transfer'] = None
            return False
        while True:
            # data_transfer_time = observation_size / self.cold.max_data_rate
            #
            # time_left = data_transfer_time - 1

            if data_left_to_transfer <= 0:
                break

            LOGGER.debug("Removing observation from buffer at time %s",
                         self.env.now)

            check = self.cold.receive_observation(
                current_obs,
                data_left_to_transfer
            )

            data_left_to_transfer = self.hot.transfer_observation(
                current_obs, self.cold.max_data_rate, data_left_to_transfer
            )

            if check != data_left_to_transfer:
                raise RuntimeError(
                    "Hot and Cold Buffer receiving data at a differen rate"
                )
            yield self.env.timeout(TIMESTEP)

        self.buffer_alloc[current_obs] = 'cold'
        return True

    def ingest_data_stream(self, observation):
        """
        Buffer ingests the data stream from the Ingest pipelines. the data
        is what is added to the 'hot' buffer every timestep
        That is - the observation.ingest_data_rate is a per-timestep value
        Timestep is the current timeout (in this scenario, it's specsoified as
        1 unit of time).
        Parameters
        ----------
        observation : core.Telescope.Observation object
            The observation we are attempting to ingest

        """
        time_left = observation.duration - 1
        if observation.status is RunStatus.WAITING:
            raise RuntimeError(
                "Observation must be marked RUNNING before ingest begins!"
            )
        while observation.status == RunStatus.RUNNING:

            self.hot.process_incoming_data_stream(
                observation.ingest_data_rate,
                self.env.now
            )
            observation.total_data_size += observation.ingest_data_rate
            if time_left > 0:
                time_left -= 1
            else:
                # observation.status = RunStatus.FINISHED
                self.waiting_observation_list.append(observation)
                self.hot.observations["stored"].append(observation)
                break

            yield self.env.timeout(TIMESTEP)

    def print_state(self):
        return {
            "hotbuffer_capacity": self.hot.current_capacity,
            "hotbuffer_stored_obsevations":
                [x.name for x in self.hot.observations["stored"]],
            "cold_buffer_storage": self.cold.current_capacity,
            "cold_buffer_observations":
                [x for x in self.cold.observations]
        }

    def to_df(self):
        """
        Return a pandas.DataFrame that represents the state of the
        Buffer and its attributes at the current timestep

        Returns
        -------
        current_state : pandas.DataFrame()
            A DataFrame (1xn) table of the current state of the Buffers.
        """
        current_state = pd.DataFrame()
        current_state['hotbuffer_total_capacity'] = [self.hot.total_capacity]
        current_state['hotbuffer_current_capacity'] = [
            self.hot.current_capacity
        ]
        current_state['hotbuffer_stored_observations'] = [len(
            self.hot.observations['stored']
        )]
        if self.hot.observations['transfer'] is not None:
            current_state['hot_transfer_observations'] = [1]
        else:
            current_state['hot_transfer_observations'] = [0]

        current_state['coldbuffer_total_capacity'] = [self.cold.total_capacity]
        current_state['coldbuffer_current_capacity'] = [
            self.cold.current_capacity
        ]
        current_state['coldbuffer_stored_observations'] = [len(
            self.cold.observations['stored']
        )]
        if self.cold.observations['transfer'] is not None:
            current_state['cold_transfer_observations'] = [1]
        else:
            current_state['cold_transfer_observations'] = [0]

        return current_state


class HotBuffer:
    """
    HotBuffer represents the ingest-facing part of the Buffer. Observation
    data is intended to stay in the HotBuffer only temporarily, and ideally
    is moved to the ColdBuffer as soon as possible.

    Transition to the ColBuffer is not instantaneous; rather it depends on
    the data rate supported by the ColdBuffer, which may be defined
    differently to the HotBuffer based on the Buffer config JSON.
    """
    def __init__(self, capacity, max_ingest_data_rate):
        self.total_capacity = capacity
        self.current_capacity = self.total_capacity
        self.max_ingest_data_rate = max_ingest_data_rate
        self.stored_observations = []
        self.observations = {
            "stored": [],
            "transfer": None
        }

    def has_waiting_observations(self):
        return bool(self.observations['stored'])

    def process_incoming_data_stream(self, incoming_datarate, time):
        """
        During Ingest, the buffer will coordinate the incoming data from
        the observation. This is a sanity check function to make sure the
        hot buffer has capacity to accept the incoming data.

        Parameters
        -----------
        incoming_datarate: The amount of data-per-timestep the telescope
        is producing


        Returns
        -----------
        True if the data can be processed - false if it cannot
        """
        if incoming_datarate > self.max_ingest_data_rate:
            raise ValueError(
                'Incoming data rate {0} exceeds maximum.'.format(
                    incoming_datarate)
            )

        self.current_capacity -= incoming_datarate
        LOGGER.debug("Current HotBuffer capacity is %s @ %s",
                     self.current_capacity, time)

        return self.current_capacity

    def transfer_observation(self, observation, transfer_rate, residual_data):
        """
        Parameters
        ----------
        Returns
        -------
        residual_data : int
            The amount of data left to transfer
        """

        if transfer_rate < 0:
            raise RuntimeError("Transfer rate must be positive")
        if self.observations['transfer'] is None:
            self.observations['transfer'] = observation
        self.current_capacity += transfer_rate
        residual_data -= transfer_rate
        if residual_data == 0:
            self.observations['transfer'] = None
        return residual_data

    def observation_for_transfer(self):
        self.observations['transfer'] = self.observations["stored"].pop()
        return self.observations['transfer']


class ColdBuffer:
    """
    Parameters
    ----------

    Methods
    --------
    has_capacity(observation_size)
        Checks that the buffer has storage capacity of observation_size

    receive_observation(observation, residual_data)
        Performs a 'per-timeste' ingest of data into the ColdBuffer.

    has_stored_observations()
        Checks if there are observations stored within the ColdBuffer

    remove(observation)
        Deletes the specified observation from observations['stored'] list
    """

    def __init__(self, capacity, max_data_rate):
        """
        The ColdBuffer takes data from the hot buffer for use in workflow
        processing
        """
        self.total_capacity = capacity
        self.current_capacity = self.total_capacity
        self.max_data_rate = max_data_rate
        self.observations = {
            'stored': [],
            'transfer': None
        }

    def has_capacity(self, observation_size):
        """
        Check if the ColdBuffer has capacity (checks self.current_capacity).

        Preferred approach over accessing self.current_capacity

        Parameters
        ----------
        observation_size : int
            The size of the observation

        Returns
        -------
        True
            If there is capacity
        False
            Otherwise.

        """
        return (
            self.current_capacity - observation_size >= 0
        )

    def receive_observation(self, observation, residual_data):
        """
        For an observation that needs to be moved to ColdBuffer storage,
        we must 'receive' it.

        Parameters
        ----------
        observation : topsim.core.telescope.Observation
            The observation to be stored

        residual_data : int
            How much data is left to transfer
        Returns
        -------
        residual_data
            Decremented value of residual_data by the data_rate of ColdBuffer
        """

        self.observations['transfer'] = observation

        self.current_capacity -= self.max_data_rate
        residual_data -= self.max_data_rate
        if residual_data == 0:
            self.observations['transfer'] = None
            self.observations['stored'].append(observation)
        return residual_data

    def has_stored_observations(self):
        """

        Hides the dictionary nature of observations stored in the Buffer.
        Returns
        -------
        True if there are observations in self.observations['stored']
        """
        return len(self.observations['stored']) > 0

    def next_observation_for_processing(self):
        """
        Produces the next observation without removal

        Returns
        -------
        topsim.core.telescope.Observation object
        """
        return self.observations['stored'][0]

    def remove(self, observation):
        """
        Removes an observation from the cold buffer and updates the capacity
        accordingly

        Parameters
        ----------
        observation : topsim.core.telescope.Observation
            The observation that is to be removed
        Returns
        -------
        True if observation is stored and remove successfully
        False otherwise
        """
        if observation in self.observations['stored']:
            self.current_capacity += observation.total_data_size
            self.observations['stored'].remove(observation)
            return True
        return False


def process_buffer_config(spec):
    """
    Reads the buffer config file and initialises HotBuffer and ColdBuffer
    objects based on the JSON data.

    Parameters
    ----------
    spec : str
        The str file path of the input JSON config file

    Raises
    ------

    Returns
    -------

    """
    try:
        with open(spec, 'r') as infile:
            config = json.load(infile)
    except OSError:
        LOGGER.warning("File %s not found", spec)
        raise
    except json.JSONDecodeError:
        LOGGER.warning("Please check file is in JSON Format")
        raise
    try:
        'hot' in config and 'cold' in config
    except KeyError:
        LOGGER.warning(
            "'system' is not in %s, check your JSON is correctly formatted",
            config
        )
        raise
    hot = HotBuffer(
        capacity=config['hot']['capacity'],
        max_ingest_data_rate=config['hot']['max_ingest_rate']
    )
    cold = ColdBuffer(
        capacity=config['cold']['capacity'],
        max_data_rate=config['cold']['max_data_rate']
    )

    return hot, cold
