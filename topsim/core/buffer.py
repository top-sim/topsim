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

import logging
import json
import pandas as pd

from topsim.common.globals import TIMESTEP
from topsim.core.telescope import RunStatus

LOGGER = logging.getLogger(__name__)


#
# class BufferQueue:
#     def __init__(self):
#         self._queue = []
#
#     def push(self, x):
#         self._queue.append(x)
#
#     def pop(self):
#         return self._queue.pop(0)
#
#     def size(self):
#         return len(self._queue)
#
#     def empty(self):
#         return len(self._queue) == 0
#


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
        self.env = env
        self.cluster = cluster
        # TODO split the hot/cold buffer into separate objects/tuples?
        # We are reading this from a file, check it works
        try:
            self.hot, self.cold = process_buffer_config(config)
        except OSError:
            raise
        self.hardware = {}
        # self.observations_for_processing = BufferQueue()
        self.buffer_alloc = {}
        self.workflow_ready_observations = []
        self.waiting_observation_list = []
        self.workflow_plans = {}
        self.new_observation = 0

    def run(self):
        while True:
            LOGGER.info(
                "HotBuffer: {}\nColdBuffer: {}".format(
                    self.hot.current_capacity,
                    self.cold.current_capacity
                )
            )
            if self.hot.has_waiting_observations():
                self.env.process(self.move_hot_to_cold())
            yield self.env.timeout(TIMESTEP)

    def check_buffer_capacity(self, observation):
        size = observation.ingest_data_rate * observation.duration
        if self.hot.current_capacity - size < 0 \
                or not self.cold.has_capacity(size):
            return False
        else:
            return True

    def ingest_data_dump(self, data):
        pass

    def has_observations_ready_for_processing(self):
        return self.cold.has_stored_observations()

    def next_observation_for_processing(self):
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

    def add(self, observation):
        LOGGER.info(
            "%s data to buffer at time %s", observation.name, self.env.now
        )
        # This will take time
        observation.plan.start_time = self.env.now
        self.waiting_observation_list.append(observation)
        LOGGER.debug('Observations in buffer %', self.waiting_observation_list)

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
        if len(self.hot.observations["stored"]) < 1:
            raise RuntimeError(
                "No observations in Hot Buffer"
            )
        current_obs = self.hot.observation_for_transfer()
        # current_obs = self.hot.observations['transfer']
        data_left_to_transfer = current_obs.total_data_size
        if not self.cold.has_capacity(data_left_to_transfer):
            return False
        while True:
            LOGGER.debug("Removing observation from buffer at time %s")
            observation_size = \
                current_obs.duration * current_obs.ingest_data_rate

            # data_transfer_time = observation_size / self.cold.max_data_rate
            #
            # time_left = data_transfer_time - 1

            if data_left_to_transfer <= 0:
                break

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
        df = pd.DataFrame()
        df['hotbuffer_total_capacity'] = [self.hot.total_capacity]
        df['hotbuffer_current_capacity'] = [self.hot.current_capacity]
        df['hotbuffer_stored_observations'] = [len(
            self.hot.observations['stored']
        )]
        if self.hot.observations['transfer']:
            df['hot_transfer_observations'] = [1]
        else:
            df['hot_transfer_observations'] = [0]

        df['coldbuffer_total_capacity'] = [self.cold.total_capacity]
        df['coldbuffer_current_capacity'] = [self.cold.current_capacity]
        df['coldbuffer_stored_observations'] = [len(
            self.cold.observations['stored']
        )]
        if self.cold.observations['transfer']:
            df['cold_transfer_observations'] = [1]
        else:
            df['cold_transfer_observations'] = [0]

        return df


class HotBuffer:
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
        if self.observations['stored']:
            return True
        else:
            return False

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
        else:
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
        # TODO Put runtime check to make sure that current_capacity does not
        #  go over total_capacity - this is possible with a negative transfer
        #  rate. 

        self.current_capacity += transfer_rate
        residual_data -= transfer_rate
        if residual_data == 0:
            self.observations['transfer'] = None
        return residual_data

    def observation_for_transfer(self):
        self.observations['transfer'] = self.observations["stored"].pop()
        return self.observations['transfer']


class ColdBuffer:
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
        return (
                self.current_capacity - observation_size >= 0
        )

    def receive_observation(self, observation, residual_data):
        self.observations['transfer'] = observation

        self.current_capacity -= self.max_data_rate
        residual_data -= self.max_data_rate
        if residual_data == 0:
            self.observations['transfer'] = None
            self.observations['stored'].append(observation)
        return residual_data

        # TODO need to yield timeout of how long it takes for the observation
        #  data to move between buffer

    def has_stored_observations(self):
        """

        Hides the dictionary nature of observations stored in the Buffer.
        Returns
        -------
        True if there are observations in self.observations['stored']
        """
        return len(self.observations['stored']) > 0

    def next_observation_for_processing(self):
        return self.observations['stored'].pop()

    def remove(self, observation):
        if observation in self.observations['stored']:
            self.current_capacity += observation.total_data_size
            self.observations['stored'].remove(observation)
            return True
        else:
            return False


def process_buffer_config(spec):
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
