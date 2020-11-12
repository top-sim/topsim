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
    def __init__(self, tid, env):
        """
        :param tid: ID of the Task object
        :param env: Simulation environment to which the task will be added, and where it will run as a process
        """
        self.id = tid
        self.env = env
        self.est = 0
        self.eft = 0
        self.ast = -1
        self.aft = -1
        self.machine_id = None
        self.duration = None
        self.exec_order = None
        self.task_status = TaskStatus.UNSCHEDULED
        self.pred = None
        self.expected_finish_time = None
        self.finished_timestamp = None
        self.started_timestamp = None
        self.delay = None

        # Machine information that is less important
        # currently (will update this in future versions)

        self.flops = 0
        self.memory = 0
        self.io = 0

    #
    # def __lt__(self, other):
    # 	return self.exec_order < other.exec_order
    #
    # def __eq__(self, other):
    # 	return self.exec_order == other.exec_order
    #
    # def __gt__(self, other):
    # 	return self.exec_order > other.exec_order

    def __hash__(self):
        return hash(self.id)

    def do_work(self):
        self.task_status = TaskStatus.RUNNING
        yield self.env.timeout(self.duration-1)
        self.finished_timestamp = self.env.now
        logger.debug('%s finished at %s', self.id, self.finished_timestamp)
        return TaskStatus.FINISHED

    # self.machine.stop_task(self)

    def run(self):
        self.started_timestamp = self.env.now
        logger.debug('%s started at %s', self.id, self.started_timestamp)
        self.task_status = TaskStatus.RUNNING
        process = self.env.process(self.do_work())
        # if process:
        #     return self.task_status
        # else:
        #     raise RuntimeError(
        #         'Task {0} failed to execute normally'.format(self))
