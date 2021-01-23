# Copyright (C) 12/1/21 RW Bunney

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

import numpy
import pandas
import seaborn

import matplotlib.pyplot as plt

graph = {
    't1': {
        'pred': [], 'succ': ['t1', 't2', 't3', 't4', 't5', 't6'],
        'est': 0, 'ast': 0, 'eft': 10, 'aft': 10
    },
    't2': {
        'pred':['t1'], 'succ':['t7'],
        'est': 11, 'ast': 11, 'eft': 25, 'aft': 25
    },  # dependent on t1
    't3': {
        'pred':['t1'],'succ':['t7'],
        'est': 15, 'ast': 15, 'eft': 28, 'aft': 28
    },  # dependent on t1
    't4': {
        'pred':['t1'], 'succ':['t8'],
        'est': 14, 'ast': 15, 'eft': 25, 'aft': 28
    },  # dependent on t1
    't5': {
        'pred': ['t1'], 'succ': ['t8'],
        'est': 12, 'ast': 12, 'eft': 28, 'aft': 28
    },  # dependent on t1
    't6': {
        'pred': ['t1'], 'succ': ['t8'],
        'est': 17, 'ast': 17, 'eft': 26, 'aft': 26
    },  # dependent on t1
    't7': {
        'est': 29, 'ast': 29, 'eft': 40, 'aft': 40
    },  # dependent on t2 & 3
    't8': {
        'est': 32, 'ast': 0, 'eft': 10, 'aft': 10
    },  # dependent on t4 & 5
    't9': {
        'est': 0, 'ast': 0, 'eft': 10, 'aft': 10
    },  # dependent on t5 & t6
    't10': {
        'est': 0, 'ast': 0, 'eft': 10, 'aft': 10
    },  # dependent on t7,t8, t9

}




def add_delay(graph, delay):
    """
    For a given task, get the current estimated and actual start and end
    times and update the AST/AFT to indicate a delay has occured within that
    task.
    Parameters
    ----------
    task: dict
        The task dictionary of estimated/actual start and finish times
    delay: int
        The integer value of the time delay on the task

    Returns
    -------
    new_times: dict
        A dictionary of new estimated/actual start and finish times for the
        graph, propogated based on which task was delayed
    """
