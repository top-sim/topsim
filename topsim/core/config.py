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
from collections import defaultdict
from pathlib import Path
from topsim.core.instrument import Observation
from topsim.core.machine import Machine
from topsim.core.buffer import HotBuffer, ColdBuffer

import logging

LOGGER = logging.getLogger(__name__)


class Config:
    """
    Process the configuration of the current simulation

    Parameters
    ----------
    config : str
        File path to the JSON config file

    Attributes
    -----------
    instrument : dict
        Data for the instrument constructor
    cluster : dict
        Data for the cluster constructor
    buffer : dict
        Data for buffer constructor
    timestep_unit: str
        String value that specifies what granualirity of time is being used
        in the simulation

    Raises
    ------
    OSError
        This is raised if we cannot read the configuration file/it does not
        exist
    json.JSONDecodeError
        This is raised if the JSON file is in the wrong format
    KeyError
        Raised if one of the attribute-keys is not found in the provided JSON
        file.
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

        self.path = Path(config)
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
        if 'timestep' in cfg and cfg['timestep'] is not None:
            self.timestep_unit = cfg['timestep']
        else:
            self.timestep_unit = 'seconds'


    def parse_cluster_config(self):
        try:
            (self.cluster['system'] and self.cluster['system']['resources'] and
             self.cluster['system']["system_bandwidth"])
        except KeyError:
            LOGGER.warning(
                "'system' is not in %s, check your JSON is correctly formatted",
                self.cluster)
            raise

        machine_list = []
        timestep_multiplier = 1
        if self.timestep_unit == 'minutes':
            timestep_multiplier = 60
        if self.timestep_unit == 'hours':
            timestep_multiplier = 3600
        elif isinstance(self.timestep_unit, int):
            # This is a custom unit
            timestep_multiplier = self.timestep_unit
        else:  # Seconds
            timestep_multiplier = timestep_multiplier

        if not self._get_machine_count():
            self._update_config()

        machines_types = self.cluster['system']['resources']
        for name, spec in machines_types.items():
            num_machines = spec.get("count")
            for i in range(num_machines):
                cpu = spec["flops"] * timestep_multiplier
                machine_list.append(
                    Machine(id=f"{name}_{i}", cpu=cpu,
                            memory=1,  # * timestep_multiplier,
                            disk=1,  # * timestep_multiplier,
                            bandwidth=(machines_types[name]["compute_bandwidth"]
                                       * timestep_multiplier)
                            )
                )
        bandwidth = self.cluster['system']["system_bandwidth"] * timestep_multiplier
        return machine_list, bandwidth

    def _get_machine_count(self):
        """
        Utility function that tries and fetches the count from the initial
        system-resources sub-dictionary.

        If there is no count, it will return None, and this is used to trigger
        the re-creation of the existing configuration file into the new format.
        """

        machines_types = self.cluster['system']['resources']
        example_key = list(machines_types.keys())[0]
        return machines_types[example_key].get("count")


    def _update_config(self):
        """
        Convert the deprecated "cluster" dictionary to the new more concise
        approach.

        Here, we make note of each unique machine 'spec'
        (flops, bandwidth, memory, and disk), count how many of them there are,
        and overwrite the existing "cluster" dictionary with the new format.

        We also over-write the existing configuration file so we do not have to
        perform this everytime we load an older configuration.

        """
        resources = self.cluster["system"]["resources"]
        grouped_specs = defaultdict(list)
        for name, spec in resources.items():
            # Use dictionary data as key to determine unique entries:
            spec["name"] = name.split("_")[0]
            spec_key = json.dumps(spec, sort_keys=True)
            grouped_specs[spec_key].append(spec)

        updated_resources = {}
        for spec_string, spec_list in grouped_specs.items():
            spec = json.loads(spec_string)
            name = spec["name"]
            del spec["name"]

            spec["count"] = len(spec_list)
            updated_resources[name] = spec
        self.cluster["system"]["resources"] = updated_resources

        with open(self.path, 'r') as fp:
            d = json.load(fp)
        with open(self.path, 'w') as fp:
            d["cluster"]["system"]["resources"] = updated_resources
            json.dump(d, fp, indent=2)

    def parse_instrument_config(self, instrument_name):
        timestep_multiplier = 1
        if self.timestep_unit == 'minutes':
            timestep_multiplier = 60
        elif self.timestep_unit == 'hours':
            timestep_multiplier = 3600
        elif isinstance(self.timestep_unit, int):
            # This is a custom unit
            timestep_multiplier = self.timestep_unit
        else:  # Seconds
            timestep_multiplier = timestep_multiplier

        cfg = self.instrument
        total_arrays = cfg[instrument_name]['total_arrays']
        pipelines = cfg[instrument_name]['pipelines']
        observations = []
        for observation in cfg[instrument_name]['observations']:
            try:
                name = observation['name']
                workflow_path = pipelines[name]['workflow']
                ingest_demand = pipelines[name]['ingest_demand']
                if 'min_workflow_resources' in observation:
                    min_resources = observation['min_workflow_resources']
                else:
                    min_resources = -1  # No minimum requirement

                if 'max_workflow_resources' in observation:
                    max_resources = observation['max_workflow_resources']
                else:
                    max_resources = -1  # No maximum resources
                o = Observation(name=name,
                    start=observation['start'] / timestep_multiplier,
                    duration=observation['duration'] / timestep_multiplier,
                    demand=observation['instrument_demand'],
                    workflow=((self.path.parent / workflow_path).as_posix()),
                    data_rate=(
                            round(observation['data_product_rate']
                            * timestep_multiplier)
                    ),
                    timestep=self.timestep_unit)
                observations.append(o)
            except KeyError:
                raise

        max_ingest_resources = cfg[instrument_name]['max_ingest_resources']
        return total_arrays, pipelines, observations, max_ingest_resources

    def parse_buffer_config(self):
        config = self.buffer
        timestep_multiplier = 1
        if self.timestep_unit == 'minutes':
            timestep_multiplier = 60
        if self.timestep_unit == 'hours':
            timestep_multiplier = 3600
        elif isinstance(self.timestep_unit, int):
            # This is a custom unit
            timestep_multiplier = self.timestep_unit
        else:  # Seconds
            timestep_multiplier = timestep_multiplier

        hot = HotBuffer(capacity=config['hot']['capacity'],
                        max_ingest_data_rate=config['hot'][
                                                 'max_ingest_rate'] *
                                             timestep_multiplier)
        cold = ColdBuffer(capacity=config['cold']['capacity'],
                          max_data_rate=config['cold'][
                                            'max_data_rate'] *
                                        timestep_multiplier)

        return {0: hot}, {0: cold}

    def get_max_ingest(self, instrument_name):

        return self.instrument[instrument_name]['max_ingest_resources']
