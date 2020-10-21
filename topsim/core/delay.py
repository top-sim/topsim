# Copyright (C) 12/10/20 RW Bunney

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

import numpy as np
from enum import Enum

class DelayModel:
    """
    The delay model is the delay or failure model for tasks in a workflow.
    If we have a possibility for delay, then the timeout triggered by a process
    will be increased by a certain portion.

    A delay model object may be passed to any of the actors within the
    simulation that yield a timeout to the environment. Each actor will store
    their expected (current) timeouts and, if present, the actual timeout
    if a delay has been triggered.
    """

    class DelayDegree(Enum):
        LOW = 0.25
        MID = 0.5
        HIGH = 0.75

    def __init__(self, probability, distribution, degree=DelayDegree.LOW):
        self.probability = probability
        self.distribution = distribution
        self.degree = degree

    def generate_delay(self, task_runtime):
        """
        Produce a delay based on current DelayModel attributes.
        A delay is a unit of time to be passed to the timeout.

        Given a probability, return the new delay by a factor of 'degree'
        Returns
        -------

        """
        delay = None
        index = self.probability
        return delay

