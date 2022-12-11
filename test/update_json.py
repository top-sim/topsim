# Copyright (C) 4/12/22 RW Bunney

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
Update JSON files with new values for data
"""

import glob
import json

with open("test/data/config/longtask/workflow_config_minutes_longtask.json") as fp:
     dict = json.load(fp)
     try:
         graph = dict["graph"]
         for node_dict in graph['nodes']:
             tmp = node_dict["comp"]
             node_dict["comp"] = 0
             node_dict["task_data"] = tmp
         with open("test/data/config/longtask/workflow_config_minutes_longtask_IO.json", "w") as fpb:
             json.dump(graph, fpb, indent=2)
         print(f"{graph['nodes'][0]['comp']}")
     except KeyError:
         print(f"Not a workflow file")
