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
from time import sleep

import pandas as pd

from tqdm import tqdm

from topsim.common.globals import TIMESTEP
from topsim.core.instrument import RunStatus

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
        config : topsim.core.config.Config
            Config object
        """
        self.env = env
        self.cluster = cluster
        # We are reading this from a file, check it works
        try:
            self.hot, self.cold = config.parse_buffer_config()
        except OSError:
            print("Error processing Buffer config file")
            raise
        self.hot_count = len(self.hot)
        self.cold_count = len(self.cold)
        self.waiting_observation_list = []
        self.events = []

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
            self.events = []
            if self.env.now % 1000 == 0:
                LOGGER.debug(
                    "\nHotBuffer: %s \nColdBuffer: %s @ %d",
                    [self.hot[b].current_capacity for b in self.hot],
                    [self.cold[b].current_capacity for b in self.cold],
                    self.env.now
                )
            for b in self.hot:
                if self.hot[b].has_waiting_observations():
                    for o in self.hot[b].observations['stored']:
                        if self.cold[b].has_capacity(
                                self.hot[b].observations['stored'][
                                    0].total_data_size
                        ):
                            self.env.process(self.move_hot_to_cold(b))
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

        TODO Ensure that we do not start observations if the size of the data +
        the total size of the TRANSFERRING observation data is > than the total
        data of the cold buffer.

        NOTE This is necessary only for if we want to move data from
        hot-to-cold. This avoids the possibility for an observation to
        go ahead when both hot buffer and cold buffer have capacity
        mid-transfer, but the cold buffer does not have capacity post-transfer.
        For example, if we both buffers have capacity of 150, we ingest a 125
        tb ingest, we start to transfer that to the buffer, then we get a 50tb
        ingest when the HotBuffer is at 80 and the ColdBuffer is at 70 (
        mid-transfer), this means will will accept an ingest of 50 data because
        both  separately are fine, but after ingest will not be.

        """
        b = observation.buffer_id
        if observation.duration < 1:
            raise RuntimeError(
                f"Observation duration has become less than 1 second.\n"
                f"Please check the observation plan, "
                f"or ensure that the conversion between observation duration "
                f"and simulation units do not cause a fractional timestep."
            )
        size = observation.ingest_data_rate * observation.duration
        if self.hot[b].total_capacity <= size:
            raise RuntimeError(
                f"Observation data size is equal or greater than HotBuffer capacity."
                f"Consider expanding capacity size."
                f"{observation.name}, Observation: {size} vs Hot Buffer: {self.hot[b].total_capacity:.1f}"
            )
        elif self.hot[b].current_capacity - size <= 0 \
                or not self.cold[b].has_capacity(size):
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
        for b in self.cold:
            if self.cold[b].has_stored_observations():
                return True
        return False
        # return self.cold.has_stored_observations()

    def next_observation_for_processing(self):
        """
        Calls the cold buffer and checks if there are any observations
        for processing

        Returns
        -------
        observation : topsim.core.telescope.Observation()
            next observation for processing
        """
        for b in self.cold:
            return self.cold[b].next_observation_for_processing()

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
        b = observation.buffer_id
        self._add_event(observation, "buffer", "removed")
        return self.cold[b].remove(observation)

    def move_hot_to_cold(self, b):
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

        # TODO Support multiple observation transfers 

        if not self.hot[b].observations["stored"]:
            raise RuntimeError(
                "No observations in Hot Buffer"
            )

        # Iterate through current observations for transfer
        # Each of them will have a data size
        # The total data rate is just total divided by the number of
        # observations in the 'transfer' dictionary.
        # Each timestep we check the length - if something has been removed
        # from transfer, we update the data rate

        current_obs = self.hot[b].observation_for_transfer()
        # current_obs = self.hot.observations['transfer']
        data_left_to_transfer = current_obs.total_data_size
        _total_data = current_obs.total_data_size
        _tqdm = False
        pbar = None
        if _tqdm:
            pbar = tqdm(total=_total_data,desc=f'Buffer: {current_obs.name}')
        if not self.cold[b].has_capacity(data_left_to_transfer):
            # We cannot actually transfer the observation due to size
            # constraints
            # TODO create an object method to update the hot buffer
            self.hot[b].observations['stored'].append(current_obs)
            self.hot[b].observations['transfer'] = None
            return False
        self._add_event(current_obs, "transfer", "started")
        while True:
            # data_transfer_time = observation_size / self.cold.max_data_rate
            #
            # time_left = data_transfer_time - 1

            if data_left_to_transfer <= 0:
                LOGGER.info(
                    "Buffer transfer completed at time %s", self.env.now
                )
                self._add_event(current_obs, "transfer", "stopped")
                break

            check = self.cold[b].receive_observation(
                current_obs,
                data_left_to_transfer
            )

            data_left_to_transfer = self.hot[b].transfer_observation(
                current_obs, self.cold[b].max_data_rate, data_left_to_transfer
            )
            if check != data_left_to_transfer:
                raise RuntimeError(
                    "Hot and Cold Buffer receiving data at a differen rate"
                )
            if pbar:
                pbar.update(n=self.cold[b].max_data_rate)
            yield self.env.timeout(TIMESTEP)
        if pbar:
            pbar.close()
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
        b = observation.buffer_id
        time_left = observation.duration - 1
        if observation.status is RunStatus.WAITING:
            raise RuntimeError(
                "Observation must be marked RUNNING before ingest begins!"
            )
        self._add_event(observation, "buffer", "added")
        while observation.status == RunStatus.RUNNING:

            self.hot[b].process_incoming_data_stream(
                observation.ingest_data_rate,
                self.env.now
            )
            observation.total_data_size += observation.ingest_data_rate
            if time_left > 0:
                time_left -= 1
            else:
                # observation.status = RunStatus.FINISHED
                self.waiting_observation_list.append(observation)
                self.hot[b].observations["stored"].append(observation)
                break

            yield self.env.timeout(TIMESTEP)

    def buffer_storage_summary(self):
        """
        Provide other actors information on the capacity and rate details of
        the respective buffers.

        This is a nicely packaged method that avoids us interacting with the
        values in the hot and cold buffers, which should not be accessed
        outside the Buffer class.

        Returns
        -------
        storage : dict
            Dictionary comprising hot and cold buffer storage information

        """
        if self.hot_count == 1 and self.cold_count == 1:
            return {
                'hotbuffer': {
                    'capacity': self.hot[0].current_capacity,
                    'data_rate': self.hot[0].max_ingest_data_rate
                },
                'coldbuffer': {
                    'capacity': self.cold[0].current_capacity,
                    'data_rate': self.cold[0].max_data_rate
                }
            }
        else:
            raise RuntimeError(f'Multi-buffer functionality not complete')

    def is_empty(self):
        """
        Determine if both buffers are cleared of all observational data

        Returns
        -------
        True if both buffers' current capacity is the same as their total
        capacity.
        """

        for buf in self.hot:
            if self.hot[buf].total_capacity != self.hot[buf].current_capacity:
                return False
        for buf in self.cold:
            if self.cold[buf].total_capacity != self.cold[buf].current_capacity:
                return False

        return True

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

        return current_state

    def _add_event(self, observation, resource, event):
        self.events.append(
            {
                "time": int(self.env.now), "actor": "buffer",
                "observation": str(observation.name), "event": str(event),
                "resource": str(resource)
            }
        )


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
        """
        Check if there are observations in the buffer to transfer to cold
        buffer.

        Returns
        -------

        """
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
        if int(incoming_datarate) > self.max_ingest_data_rate:
            raise ValueError(
                'Incoming data rate {0} exceeds maximum.'.format(
                    incoming_datarate)
            )

        self.current_capacity -= incoming_datarate
        # LOGGER.debug("Current HotBuffer capacity is %s @ %s",
        #              self.current_capacity, time)

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

        if self.observations['transfer'] is None:
            self.observations['transfer'] = observation
        # We are doing a 'real-time' simulation, which means we treat the hot
        # and cold buffers as one buffer.

        if transfer_rate < 0:
            self.current_capacity += observation.total_data_size
            residual_data -= observation.total_data_size
        elif residual_data < transfer_rate:
            self.current_capacity += residual_data
            residual_data = 0
        else:
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

    def __init__(self, capacity, max_data_rate, env=None):
        """
        The ColdBuffer takes data from the hot buffer for use in workflow
        processing
        """
        self.total_capacity = capacity
        self.current_capacity = self.total_capacity
        self.max_data_rate = max_data_rate
        self.next_obs = 0
        self.observations = {
            'stored': [],
            'transfer': None
        }
        self.env = env

    def has_capacity(self, observation_size):
        """
        Check if the ColdBuffer has capacity (checks self.current_capacity).

        Preferred approach over accessing self.current_capacity

        We also need to check that the combined observation size of the capacity

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
        size = observation_size
        if self.observations['transfer']:
            size = observation_size + self.observations[
                'transfer'].total_data_size

        return (
                self.current_capacity - size >= 0
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

        if self.max_data_rate > 0:
            if residual_data < self.max_data_rate:
                self.current_capacity -= residual_data
                residual_data = 0
            else:
                self.current_capacity -= self.max_data_rate
                residual_data -= self.max_data_rate

        else:
            self.current_capacity -= observation.total_data_size
            residual_data -= observation.total_data_size

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
        if len(self.observations['stored']) >= self.next_obs:
            return self.observations['stored'][-1]
        else:
            obs = self.observations['stored'][self.next_obs]
            self.next_obs += 1
            return obs

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

