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
Parsing logic to support the runtime.cli module.

This is to improve the 'cleanliness' of that module, as it requires lots
of preambles for each of ther grouped commands that have 'options'.
"""
import enum
import inspect

from importlib import import_module

class DataUse(enum.Enum):
    NoData = "NoData"
    TaskData = "TaskData"
    EdgeData = "EdgeData"
    TaskEdgeData = "TaskEdgeData"

    def __str__(self):
        return self.value

DATA_USE_MAP = {
    str(DataUse.NoData): {'use_task_data': False, 'use_edge_data': False},
    str(DataUse.TaskData): {'use_task_data': True, 'use_edge_data': False},
    str(DataUse.EdgeData): {'use_task_data': False, 'use_edge_data': True},
    str(DataUse.TaskEdgeData): {'use_task_data': True, 'use_edge_data': True}
}

def import_class(module_path: str):
    """
    Dynamically import a class from a module path string.
    """
    module_name, class_name = module_path.rsplit(".", maxsplit=1)
    module = import_module(module_name)
    cls =  getattr(module, class_name)
    if inspect.isclass(cls):
        return cls
    else:
        raise RuntimeError(f"You did not pass a class {cls}, cannot instantialise object!, cls")


def parse_experiment(planning, scheduling, data):
    """
    Build combinations of planning, scheduling, and data experiments.

    Parameters
    ----------
    planning: list, planning modules
    scheduling: list, scheduling modules
    data:

    Returns
    -------
    allocator_combinations, data_combinations
    """

    plan_modules = []
    for p in planning:
        plan_modules.append(import_class(p))

    scheduling_modules = []
    for s in scheduling:
        scheduling_modules.append(import_class(s))

    allocator_combinations = []
    for i in range(min(len(plan_modules), len(scheduling_modules))):
        allocator_combinations.append((plan_modules[i],
                                       scheduling_modules[i]))

    data_combinations = []
    for d in data:
        data_combinations.append(DATA_USE_MAP[d])

    return allocator_combinations, data_combinations