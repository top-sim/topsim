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

from topsim.core.planning import Planning


class BatchPlanning(Planning):

    def __init__(self, observation, model, buffer):
        super().__init__(observation, model, buffer)

    def generate_plan(self):
        if algorithm is 'batch':
            self.est = self._calc_workflow_est(observation, buffer)
            self.eft = -1
            self.tasks = workflow

        pass

    def to_df(self):
        pass