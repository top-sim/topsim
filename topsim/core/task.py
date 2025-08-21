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


import simpy
import logging
import copy
import math
import numpy as np
from enum import Enum

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    UNSCHEDULED = 1
    SCHEDULED = 2
    RUNNING = 3
    FINISHED = 4


class Task(object):
    """
    Tasks have priorities inheritted from the workflows from which they are
    arrived; once they arrive at the cluster queue, they are workflow , and
    are processed according to their priority.
    """

    # NB I don't want tasks to have null defaults; should we improve on this
    # by initialising everything in a task at once?
    def __init__(self, tid, est, eft, machine_id, predecessors, flops=0, task_data=0,
                 edge_data: dict=None, delay=None, gid=None, use_task_data=False, use_edge_data=True):
        """
        :param tid: ID of the Task object

        """

        self.id = tid
        self.est = est
        self.eft = eft
        self.ast = -1
        self.aft = -1
        self.allocated_machine_id = machine_id
        self.duration = eft - est  # TODO investigate making this a class property
        self.est_duration = eft - est
        self.delay_flag = False
        self.task_status = TaskStatus.UNSCHEDULED
        self.pred = predecessors
        self.delay = delay
        self.delay_offset = 0
        self.workflow_offset = 0
        self.graph_id = gid


        # Used to calculate actual runtime on the system
        self.flops = flops
        self.task_data = task_data
        self.edge_data = edge_data  # Input/edge data

        self.use_edge_data = use_edge_data
        self.use_task_data = use_task_data


    def __repr__(self):
        return str(self.id)

    def __hash__(self):
        return hash(self.id)

    def do_work(self, env, machine, predecessor_allocations=None):
        """
        This runs the task on the 'cluster'. We make the task in control of
        it's execution in order to "give it control" of delays.

        do_work() follows a trend of TOpSim code in that it yields and then
        returns after a given duration.
        Parameters
        ----------
        predecessor_allocations
        machine
        env
        alt = True if the machine is different to the predecessors machine.
        This affects data transfer between tasks.

        Returns
        -------

        """
        if predecessor_allocations:
            yield env.timeout(
                self._wait_for_transfer(env, machine, predecessor_allocations))
        self.task_status = TaskStatus.RUNNING
        self.ast = env.now

        # self.eft = self.duration+self.ast
        # Process potential updates to duration:
        if (self.flops > 0) or (self.task_data > 0):
            self.duration = self.calculate_runtime(machine)
        total_duration = self._calc_task_delay()
        if total_duration < 1:
            yield env.timeout(0)
        else:
            yield env.timeout(total_duration - 1)

        if self.duration < total_duration:
            self.delay_flag = True
            self.delay_offset += (total_duration - self.duration)

        self.aft = env.now + 1
        if self.aft > self.eft:
            self.delay_flag = True
        # self.task_status = TaskStatus.FINISHED
        # print(total_duration, self.aft-self.ast)

        logger.debug('%s finished at %s', self.id,
                     self.aft)  # return TaskStatus.FINISHED

    def calculate_runtime(self, machine):
        """
        Calculate the runtime of the task

        The duration is a function of either compute-time (FLOPs) or data-time (bandwidth);
        we use whichever is greater.

        Parameters
        ----------
        machine: The machine to which we are allocated (passed in during do_work())

        Returns
        -------
        Maximum integer time of either compute- or data-time
        """

        compute_time = int(self.flops / machine.cpu)
        data_time = int(self.task_data / machine.bandwidth) if self.use_task_data else 0
        return max(compute_time, data_time)

    def update_allocation(self, machine):
        """
        At runtime, it may be that a machine allocation is suggested that we
        have not planned for. Therefore, we need to recalculate the duration
        based on that allocation.

        Additionally, it is possible that the new allocation increases the
        time cost associated with the task (that is, the duration is longer
        than originally expected). This means we have a form of runtime delay
        that is separate to the 'average' delay likelihood built into running
        tasks. Therefore, we need to update the tasks actual duration,
        whilst also keeping the old value of duration to help determine the
        extent to which the task is delayed, both based on this machine
        re-allocation *and* the task runtime variations.

        Parameters
        ----------
        machine
            The new machine we are updating to.

        Returns
        -------

        """
        self.allocated_machine_id = machine
        compute_time = int(self.flops / machine.cpu)
        data_time = int(self.task_data / machine.bandwidth)
        duration = max(compute_time, data_time)
        if duration > self.duration:
            self.delay_flag = True
            self.delay_offset = (duration - self.duration)
            self.duration = duration

    def _wait_for_transfer(self, env, machine, predecessor_allocations):
        """
        Get the maximum data value from self.predecessors and use this to
        calculate the data transfer for a task.

        Returns maximum predecessor data transfer
        -------
        """

        mx = 0
        if not self.use_edge_data:
            return mx
        # Calculate the difference between the latest start time of the
        # predecessor and the current time.
        for task in predecessor_allocations:
            transfer_time = self.edge_data[task.id] / machine.ethernet
            if task.aft + transfer_time - env.now > mx:
                mx = task.aft + transfer_time - env.now
        return mx

    def _calc_task_delay(self):
        """
        Use the delay model associated with the task to generate a delay
        Returns
        -------
        updated duration
        """
        if self.delay is not None:
            return self.delay.generate_delay(self.duration)
        else:
            return self.duration

    def _determine_bottleneck(self):
        """
        Stub : will eventually check for which is slower, the FLOPS or the
        memory bandwidth.
        Returns
        -------

        """
        return None
