# Copyright (C) 2020 RW Bunney

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


import json
from topsim.core.instrument import Observation
from topsim.core.machine import Machine
from topsim.core.buffer import HotBuffer, ColdBuffer
import pandas as pd

import logging

LOGGER = logging.getLogger(__name__)


# class Workflow(object):
# 	def __init__(self,workflow):
# 		self.id = workflow
# 		self.submit_time = 0
#

class Config:
    """
    Process the configuration of the current simulation
    """

    def __init__(self, config):
        try:
            with open(config, 'r') as infile:
                cfg = json.load(infile)
        except OSError:
            LOGGER.warning("File %s not found", config)
            raise
        except json.JSONDecodeError:
            raise

        if 'instrument' in cfg and cfg['instrument'] is not None:
            self.instrument = cfg['instrument']
        else:
            raise KeyError("'instrument' is not present in JSON")
        if 'cluster' in cfg and cfg['cluster'] is not None:
            self.cluster = cfg['cluster']
        else:
            raise KeyError("'cluster' is not present in JSON")
        if 'buffer' in cfg and cfg['buffer'] is not None:
            self.buffer = cfg['buffer']
        else:
            raise KeyError("'buffer' is not present in JSON")
        if 'planning' in cfg and cfg['planning'] is not None:
            self.planning = cfg['planning']
        else:
            raise KeyError("'planning' is not present in JSON")
        if 'scheduling' in cfg and cfg['scheduling'] is not None:
            self.scheduling = cfg['scheduling']
        else:
            raise KeyError("'scheduling' is not present in JSON")

        if 'timestep' in cfg and cfg['timestep'] is not None:
            self.timestep_unit = cfg['timestep']
        else:
            self.timestep_unit = 'seconds'

    def parse_cluster_config(self):
        try:
            (self.cluster['system'] and self.cluster['system']['resources']
             and self.cluster['system']['bandwidth'])
        except KeyError:
            LOGGER.warning(
                "'system' is not in %s, check your JSON is correctly formatted",
                self.cluster
            )
            raise

        machines = self.cluster['system']['resources']
        machine_list = []
        timestep_multiplier = 1
        if self.timestep_unit == 'minutes':
            timestep_multiplier = 60
        if self.timestep_unit == 'hours':
            timestep_multiplier = 3600
        for machine in machines:
            cpu = machines[machine]['flops']*timestep_multiplier
            machine_list.append(
                Machine(
                    id=machine,
                    cpu=cpu,
                    memory=1,# * timestep_multiplier,
                    disk=1,# * timestep_multiplier,
                    bandwidth=machines[machine]['rates'] * timestep_multiplier
                )
            )

        bandwidth = self.cluster['system']['bandwidth'] * timestep_multiplier
        return machine_list, bandwidth

    def parse_instrument_config(self, instrument_name):
        cfg = self.instrument
        total_arrays = cfg[instrument_name]['total_arrays']
        pipelines = cfg[instrument_name]['pipelines']
        observations = []
        for observation in cfg[instrument_name]['observations']:
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
        max_ingest_resources = cfg[instrument_name]['max_ingest_resources']
        return total_arrays, pipelines, observations, max_ingest_resources

    def parse_buffer_config(self):
        config = self.buffer
        hot = HotBuffer(
            capacity=config['hot']['capacity'],
            max_ingest_data_rate=config['hot']['max_ingest_rate']
        )
        cold = ColdBuffer(
            capacity=config['cold']['capacity'],
            max_data_rate=config['cold']['max_data_rate']
        )

        return {0: hot}, {0: cold}


class TaskInstanceConfig(object):
    def __init__(self, task_config):
        self.cpu = task_config.cpu
        self.memory = task_config.memory
        self.disk = task_config.disk
        self.duration = task_config.duration


class TaskConfig(object):
    def __init__(self, task_index, instances_number, cpu, memory, disk,
                 duration):
        self.task_index = task_index
        self.instances_number = instances_number
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.duration = duration


class JobConfig(object):
    def __init__(self, idx, submit_time, task_configs):
        self.submit_time = submit_time
        self.task_configs = task_configs
        self.id = idx

    def __repr__(self):
        return str(self.id)
