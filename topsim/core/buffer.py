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
from topsim.common.globals import TIMESTEP
from topsim.core.telescope import RunStatus

logger = logging.getLogger(__name__)

BUFFER_OFFSET = topsim.common.globals.BUFFER_TIME_OFFSET


class BufferQueue:
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
        self.observations_for_processing = BufferQueue()
        self.buffer_alloc = {}
        self.workflow_ready_observations = []
        self.waiting_observation_list = []
        self.workflow_plans = {}
        self.new_observation = 0

    def run(self):
        while True:
            if self.hot.has_waiting_observations():
                self.move_hot_to_cold()
            yield self.env.timeout(1)

    def check_buffer_capacity(self, observation):
        size = observation.ingest_data_rate * observation.duration
        if self.hot.current_capacity - size < 0 \
                and self.cold.current_capacity - size < 0:
            return False
        else:
            return True

    def ingest_data_dump(self, data):
        pass

    def move_hot_to_cold(self):

        return True

    def has_observations_ready_for_processing(self):

        return True

    def get_observations_ready_for_processing(self):
        l = []
        return l

    def add(self, observation):
        logger.info(
            "%s data to buffer at time %s", observation.name, self.env.now
        )
        # This will take time
        observation.plan.start_time = self.env.now + BUFFER_OFFSET
        self.waiting_observation_list.append(observation)
        logger.debug('Observations in buffer %', self.waiting_observation_list)

    def request_data_from(self, observation):
        """

        TODO this should change
        Called when the scheduler is requesting data for workflow processing.

        This method 'moves' the observation data from the HotBuffer to the
        ColdBuffer, at a rate of  ColdBuffer.max_data_rate.

        ----------
        observation : core.telescope.Observation object

            The observation that is stored in the HotBuffer, to be moved

        Returns
        -------

        """
        logger.debug("Removing observation from buffer at time %s")
        if observation not in self.waiting_observation_list:
            raise RuntimeError
        observation_size = observation.duration * observation.ingest_data_rate
        data_transfer_time = observation_size / self.cold.max_data_rate

        time_left = data_transfer_time - 1
        while True > 0:
            self.hot.current_capacity += self.cold.max_data_rate
            self.cold.current_capacity -= self.cold.max_data_rate

            if time_left > 0:
                time_left -= 1
            else:
                break

            yield self.env.timeout(TIMESTEP)

        self.cold.observations.append(observation)
        self.buffer_alloc[observation] = 'cold'

    def print_state(self):
        return {
            'workflow ready obs': self.workflow_ready_observations,
            'hotbuffer_capacity': self.hot.current_capacity,
            'hotbuffer_stored_obsevations':
                [x.name for x in self.hot.stored_observations],
            'cold_buffer_storage': self.cold.current_capacity,
            'cold_buffer_observations':
                [x.name for x in self.cold.observations]
        }

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

            hotbuffer_capacity = self.hot.process_incoming_data_stream(
                observation.ingest_data_rate,
                self.env.now
            )
            if time_left > 0:
                time_left -= 1
            else:
                break

            yield self.env.timeout(
                1, value=hotbuffer_capacity
            )

        self.waiting_observation_list.append(observation)
        self.hot.stored_observations.append(observation)


class HotBuffer:
    def __init__(self, capacity, max_ingest_data_rate):
        self.total_capacity = capacity
        self.current_capacity = self.total_capacity
        self.max_ingest_data_rate = max_ingest_data_rate
        self.stored_observations = []

    def has_waiting_observations(self):
        if self.stored_observations:
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
            logger.debug("Current HotBuffer capacity is %s @ %s",
                         self.current_capacity, time)

        return self.current_capacity


class ColdBuffer:
    def __init__(self, capacity, max_data_rate):
        """
        The ColdBuffer takes data from the hot buffer for use in workflow
        processing
        """
        self.total_capacity = capacity
        self.current_capacity = self.total_capacity
        self.max_data_rate = max_data_rate
        self.observations = []

    def add_observation(self, observation):
        self.observations.append(observation)
        self.current_capacity =- observation.total_data_size
        # TODO need to yield timeout of how long it takes for the observation
        #  data to move between buffer

    def request_hot_observations(self):

        return None

    def process_data(self):
        return None


def process_buffer_config(spec):
    try:
        with open(spec, 'r') as infile:
            config = json.load(infile)
    except OSError:
        logger.warning("File %s not found", spec)
        raise
    except json.JSONDecodeError:
        logger.warning("Please check file is in JSON Format")
        raise
    try:
        'hot' in config and 'cold' in config
    except KeyError:
        logger.warning(
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
