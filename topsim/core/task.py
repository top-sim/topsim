# Copyright (C) 8/9/20 RW Bunney

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

from enum import Enum
from topsim.core.delay import DelayModel
import logging

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    UNSCHEDULED = 1
    SCHEDULED = 2
    RUNNING = 3
    FINISHED = 4


class Task(object):
    """
    Tasks have priorities inheritted from the workflows from which they are arrived; once
    they arrive on the cluster queue, they are workflow agnositc, and are processed according to
    their priority.
    """

    # NB I don't want tasks to have null defaults; should we improve on this
    # by initialising everything in a task at once?
    def __init__(self, tid):
        """
        :param tid: ID of the Task object
        :param env: Simulation environment to which the task will be added, and where it will run as a process
        """

        self.id = tid
        self.est = 0
        self.eft = 0
        self.ast = -1
        self.aft = -1
        self.machine_id = None
        self.duration = None
        self.task_status = TaskStatus.UNSCHEDULED
        self.pred = None
        self.delay = None

        # Machine information that is less important
        # currently (will update this in future versions)

        self.flops = 0
        self.memory = 0
        self.io = 0

    def __repr__(self):
        return str(self.id)

    def __hash__(self):
        return hash(self.id)

    def do_work(self,env):
        """
        This runs the task on the 'cluster'. We make the task in control of
        it's execution in order to "give it control" of delays.

        do_work() follows a trend of TOpSim code in that it yields and then
        returns after a given duration.
        Parameters
        ----------
        env

        Returns
        -------

        """
        self.task_status = TaskStatus.RUNNING
        self.ast = env.now
        yield env.timeout(self.duration-1)
        self.aft = env.now
        logger.debug('%s finished at %s', self.id, self.aft)
        return TaskStatus.FINISHED
