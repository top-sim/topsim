# Copyright (C) 5/1/21 RW Bunney

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
Unit tests for the Config class. Test configuration for all
"""

import unittest
import json

from topsim.core.config import Config
from topsim.user.telescope import Telescope

CONFIG = "test/data/config/standard_simulation_longtask.json"
NOFILE = "test/data/config/cluster_config.json"  # Does not exist
INCORRECT_JSON = "test/data/config/sneaky.json"
NOT_JSON = "test/data/config/oops.txt"
MISSING_KEYS = "test/data/config/config_missing_keys.json"
CONFIG_CUSTOM_TIMESTEP = "test/data/config/custom_timestep.json"


class TestGeneralConfig(unittest.TestCase):

    def test_config_file_exists(self):
        config = Config(CONFIG)

    def test_config_nofile(self):
        self.assertRaises(FileNotFoundError, Config, NOFILE)

    def test_config_not_json(self):
        self.assertRaises(json.JSONDecodeError, Config, NOT_JSON)

    def test_config_incorrect_json(self):
        self.assertRaises(KeyError, Config, INCORRECT_JSON)


class TestActorConfigIncorrectJSON(unittest.TestCase):

    def setUp(self):
        self.config = Config(MISSING_KEYS)

    def test_cluster_config_incorrect_json(self):
        self.assertRaises(KeyError, self.config.parse_cluster_config)

    def test_instrument_config_incorrect_json(self):
        self.assertRaises(KeyError, self.config.parse_instrument_config,
                          Telescope.name)

    def test_buffer_config_incorrect_json(self):
        self.assertRaises(KeyError, self.config.parse_buffer_config)


class TestActorConfigurationReturnsCorrectDictionary(unittest.TestCase):

    def setUp(self):
        self.config = Config(CONFIG)

    def test_instrument_parameters(self):
        (total_arrays, pipelines, observations,
         max_ingest_resources) = self.config.parse_instrument_config(
            Telescope.name)

        self.assertTrue('emu' in pipelines)  # self.assertTrue('')


class TestConfigTimeStep(unittest.TestCase):

    def setUp(self) -> None:
        """
        This tests the different configuration options and their requisite
        calculations for per-timestep values.

        Alternative timesteps were a retroactively applied feature,
        so to reduce complications and avoid re-generating a bunch of
        workflows and config files, the approach taken was to keep the total
        data products produced per-config the same (and therefore have less
        data produced/second for our "minutes" config) to make it easier to
        integrate existing values used in tests.

        Returns
        -------

        """
        self.standard_config = "test/data/config/integration_simulation.json"
        self.minutes_config = "test/data/config/standard_simulation.json"
        self.custom_config = (
            "test/data/config/standard_simulation_custom_timestep.json"
        )
        self.observation_duration_in_timesteps = 20
        self.observation_data_product_rate_per_timestep = 5000000000.0  #
        # bytes/sec
        self.hot_buffer_ingest_rate_per_timestep = 5000000000.0
        self.cold_buffer_data_rate_per_timestep = 2000000000.0

    def testConfigDefaultTimestep(self):
        """
        Assume 1 timestep is 1 second; this means we do not adjust
        the values in the config files.

        Returns
        -------

        """
        config = Config(self.standard_config)
        (total_arrays, pipelines, observations,
         max_ingest_resources) = config.parse_instrument_config("telescope")
        self.assertAlmostEqual(
            self.observation_duration_in_timesteps, observations[1].duration
        )
        self.assertAlmostEqual(
            self.observation_data_product_rate_per_timestep,
            observations[1].ingest_data_rate, places=5
        )
        hot, cold = config.parse_buffer_config()

        self.assertAlmostEqual(
            self.hot_buffer_ingest_rate_per_timestep,
            hot[0].max_ingest_data_rate, places=5
        )
        self.assertAlmostEqual(
            self.cold_buffer_data_rate_per_timestep,
            cold[0].max_data_rate,places=5
        )

    def testConfigMinutesTimestep(self):
        config = Config(self.minutes_config)
        (total_arrays, pipelines, observations,
         max_ingest_resources) = config.parse_instrument_config("telescope")
        self.assertEqual(
            self.observation_duration_in_timesteps, observations[1].duration
        )
        self.assertAlmostEqual(
            self.observation_data_product_rate_per_timestep,
            observations[1].ingest_data_rate,places=5
        )
        hot, cold = config.parse_buffer_config()

        self.assertAlmostEqual(
            self.hot_buffer_ingest_rate_per_timestep,
            hot[0].max_ingest_data_rate, places=5
        )
        self.assertAlmostEqual(
            self.cold_buffer_data_rate_per_timestep,
            cold[0].max_data_rate, places=5
        )

    def testConfigCustomTimestep(self):
        config = Config(self.custom_config)
        (total_arrays, pipelines, observations,
         max_ingest_resources) = config.parse_instrument_config("telescope")
        self.assertEqual(
            self.observation_duration_in_timesteps, observations[1].duration
        )
        self.assertAlmostEqual(
            self.observation_data_product_rate_per_timestep,
            observations[1].ingest_data_rate
        )
        hot, cold = config.parse_buffer_config()

        self.assertAlmostEqual(
            self.hot_buffer_ingest_rate_per_timestep,
            hot[0].max_ingest_data_rate
        )
        self.assertAlmostEqual(
            self.cold_buffer_data_rate_per_timestep,
            cold[0].max_data_rate
        )
