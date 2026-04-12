# Copyright (C) 2025 RW Bunney

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
Test runtime.commands and runtime.parser
"""

import unittest

from topsim.runtime.parser import import_class, parse_experiment
from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.schedule.dynamic_plan import DynamicSchedulingFromPlan
from topsim.user.plan.batch_planning import BatchPlanning
from topsim.user.plan.static_planning import SHADOWPlanning

class TestImportClass(unittest.TestCase):

    def test_import_class(self):
        """
        Confirm we import classes correctly
        """
        class_exists = "topsim.user.schedule.batch_allocation.BatchProcessing"
        cls = import_class(class_exists)
        self.assertEqual(BatchProcessing, cls)

    def test_import_class_doesnt_exist(self):

        class_fake = "topsim.user.schedule.fake_allocation.TestFake"
        self.assertRaises(ModuleNotFoundError, import_class, class_fake)

    def test_import_module_node_class(self):

        module = "topsim.user.schedule.batch_allocation"
        self.assertRaises(RuntimeError, import_class, module)

class TestExperimentParser(unittest.TestCase):

    planning = ["topsim.user.plan.batch_planning.BatchPlanning",
                "topsim.user.plan.static_planning.SHADOWPlanning"]

    scheduling = ["topsim.user.schedule.batch_allocation.BatchProcessing",
                  "topsim.user.schedule.dynamic_plan.DynamicSchedulingFromPlan"]

    data = ["NoData", "TaskEdgeData", "EdgeData"]

    def test_parse_experiment(self):

        allocator_combinations, data_combinations = parse_experiment(self.planning,
                                                                     self.scheduling,
                                                                     self.data)
        exp_alloc_comb = [(BatchPlanning, BatchProcessing),
                          (SHADOWPlanning, DynamicSchedulingFromPlan)]
        exp_data_comb = [ {'use_task_data': False, 'use_edge_data': False},
                              {'use_task_data': True, 'use_edge_data': True},
                              {'use_task_data': False, 'use_edge_data': True}]
        self.assertListEqual(exp_alloc_comb, allocator_combinations)
        self.assertListEqual(exp_data_comb, data_combinations)