# Copyright (C) 12/7/21 RW Bunney

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

from topsim.core.algorithm import Algorithm


class BatchProcessing(Algorithm):
    """
    Dynamic schedule for a workflowplan that is using a batch-processing
    resource reservation approach without generating a static schedule.
    """
    def __init__(self, threshold=0.8):
        self.threshold = threshold

    def __repr__(self):
        return "BatchProcessing"

    def __call__(self, cluster, clock, workflow_plan,existing_schedule):
        pass

    def to_df(self):
        pass