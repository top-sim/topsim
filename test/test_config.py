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

CONFIG = "test/data/config/standard_simulation.json"
NOFILE = "test/data/config/cluster_config.json"  # Does not exist
INCORRECT_JSON = "test/data/config/sneaky.json"
NOT_JSON = "test/data/config/oops.txt"
MISSING_KEYS = "test/data/config/config_missing_keys.json"

class TestGeneralConfig(unittest.TestCase):

    def test_config_file_exists(self):
        config = Config(CONFIG)


    def test_config_nofile(self):
        self.assertRaises(FileNotFoundError, Config, NOFILE)

    def test_config_not_json(self):
        self.assertRaises(
            json.JSONDecodeError, Config, NOT_JSON
        )

    def test_config_incorrect_json(self):
        self.assertRaises(
            KeyError, Config, INCORRECT_JSON
        )


class TestActorConfigIncorrectJSON(unittest.TestCase):

    def setUp(self):
        self.config = Config(MISSING_KEYS)

    def test_cluster_config_incorrect_json(self):

        self.assertRaises(
            KeyError, self.config.parse_cluster_config
        )

    def test_instrument_config_incorrect_json(self):
        self.assertRaises(
            KeyError, self.config.parse_instrument_config, Telescope.name
        )

    def test_buffer_config_incorrect_json(self):
        self.assertRaises(
            KeyError, self.config.parse_buffer_config
        )
