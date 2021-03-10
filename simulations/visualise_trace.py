# Copyright (C) 10/9/20 RW Bunney

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
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


sns.set_style("darkgrid")
# tracefile = 'simulations/heft_sim/output/sim.trace'
# heft_pickle = 'simulations/real_time/real_time.trace-heft.pkl'
heft_pickle = 'simulations/heft_sim/output/heft_sim.trace-heft.pkl'
delay_heft_pickle = 'simulations/heft_sim/output/heft_sim_delay.trace-heft.pkl'
delay_low_heft_pickle = 'simulations/heft_sim/output/heft_sim_delay_low.trace' \
                       '-heft.pkl'
# with open(tracefile, 'r') as infile:
# 	trace = json.load(infile)

df_heft = pd.read_pickle(heft_pickle)
df_delay = pd.read_pickle(delay_heft_pickle)
df_low_delay = pd.read_pickle(delay_low_heft_pickle)

# fig, axs = plt.subplots(nrows=1, ncols=2)

sns.lineplot(
    data=df_heft, x=df_heft.index, y="running_tasks",label='No Delay'
)
sns.lineplot(
    data=df_delay,x=df_delay.index, y="running_tasks", label='High Delay'
)
sns.lineplot(
    data=df_low_delay, x=df_low_delay.index, y="running_tasks", label='Low '
                                                                      'Delay'
)

# sns.lineplot(
#     data=df_heft, x=df_heft.index, y="available_resources", ax=axs[1]
# )
#
# sns.lineplot(
#     data=df_delay,x=df_delay.index, y="available_resources", ax=axs[0,1]
# )
#
# sns.lineplot(
#     data=df_heft, x=df_heft.index, y="ingest_resources", ax=axs[0, 1]
# )

plt.savefig("delays.svg", format='svg')

# for timestamp in trace:
# 	# print('Time @ {}'.format(timestamp['timestamp']))
#
# 	print('\tcluster_state:')
# 	for element in timestamp['cluster_state']:
# 		for m in timestamp['cluster_state']['machines']:
# 			print('\t\t{}'.format(m))
# 	print('\ttelescope_state:')
# 	for element in timestamp['telescope_state']:
# 		print('\t\t{}: {}'.format(
# 			element, timestamp['telescope_state'][element]
# 		))
# 	print('\tscheduler_state:')
# 	print('\t\t{}'.format(timestamp['scheduler_state']))
# 	print('\t{}'.format(timestamp['buffer_state']))
# 	for element in timestamp['buffer_state']:
# 		print('\t\t{}'.format(timestamp['buffer_state'][element]))
#
