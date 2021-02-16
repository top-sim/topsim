# Copyright (C) 16/2/21 RW Bunney

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
Unittests for the topsim.core.delay.DelayModel class
"""

import unittest
from numpy.random import seed

from topsim.core.delay import DelayModel


class TestDelayCreation(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_initialisation(self):

        dm = DelayModel(0.1, "normal")
        self.assertEqual('normal', dm.dist)
        self.assertEqual(0.1, dm.prob)
        # Non-existent/not implemneted distribution defaults to uniform
        dm = DelayModel(0.1, "pink_panther")
        self.assertEqual('uniform', dm.dist)

    def test_random_variable_generation(self):
        # Create a sample task runtime
        dm = DelayModel(0.1, "normal")
        # Ensure default seed is the same as our expectations
        self.assertEqual(20, dm.seed)
        rt = 10
        var = dm._create_random_value_from_runtime(rt)
        self.assertAlmostEqual(11.30,var,2)

    def test_delay_generation(self):
        dm = DelayModel(0.1, "normal")
        rt = 10
        delay = dm.generate_delay(rt)
        # The probability is too low with the given seed, so we don't have a
        # delay
        self.assertEqual(0, delay-rt)
        dm = DelayModel(0.3, "normal")
        delay = dm.generate_delay(rt)
        self.assertEqual(1, delay-rt)

