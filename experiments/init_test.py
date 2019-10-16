# Copyright (C) 2018 RW Bunney

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

from core.machine import MachineConfig
from _archive.playground.utils.episode import Episode
from _archive.playground.algorithm import RandomAlgorithm
from _archive.playground.utils.csv_reader import CSVReader
import time
import sys
from _archive.playground.utils.tools import average_completion, average_slowdown
sys.path.append('..')

machines_number = 5
jobs_len = 5
n_iter = 50  # Original code was 100
n_episode = 12
jobs_csv = "workflows.csv"

machine_configs = [MachineConfig(64, 1, 1) for i in range(machines_number)]
csv_reader = CSVReader(jobs_csv)
jobs_configs = csv_reader.generate(0, jobs_len)

tic = time.time()
algorithm = RandomAlgorithm()
episode = Episode(machine_configs, jobs_configs, algorithm, 'event_{0}.trace'.format(jobs_len) )
episode.run()
print("Random Algorithm")
print(episode.env.now, time.time() - tic, average_completion(episode), average_slowdown(episode))
