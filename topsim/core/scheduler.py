# Copyright (C) 10/19 RW Bunney

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
import logging
import pandas as pd

from enum import Enum

from topsim.common.globals import TIMESTEP
from topsim.core.instrument import RunStatus
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

LOGGER = logging.getLogger(__name__)


class Scheduler:
    """

    Parameters
    ----------
    env : Simpy.Environment object
        The simulation Environment

    buffer : core.buffer.Buffer object
        The SDP buffer used in the simulation

    cluster : core.cluster.Cluster object
        The Cluster instance used for the simluation

    Attributes
    ----------
    """

    def __init__(self, env, buffer, cluster, algorithm):
        self.env = env
        self.algorithm = algorithm
        self.cluster = cluster
        self.buffer = buffer
        self.status = SchedulerStatus.SLEEP
        self.ingest_observation = None
        self.observation_queue = []
        self.schedule_status = ScheduleStatus.ONTIME
        self.delay_offset = 0

    def start(self):
        """
        Set the SchedulerStatus to RUNNING.
        This allows us to check the Scheduler status in the simulator; if
        there is nothing left the schedule, we may finish the simulation.

        Returns
        -------
        self.status : topsim.core.scheduler.SchedulerStatus
            SchedulerStatus.RUNNING for all calls to init()
        """
        self.status = SchedulerStatus.RUNNING
        return self.status

    def shutdown(self):
        self.status = SchedulerStatus.SHUTDOWN
        return self.status

    def run(self):
        """
        Starts the 'per-TIMESTEP' process loop for the Scheduler actor.

        The Scheduler coordinates interaction between the Telescope, Buffer,
        and Cluster. The Telescope should not *see* the Buffer or Cluster; all
        communication must be transferred through the scheduler.

        Yields
        -------
        Timeout of common.config.TIMESTEP.
        """

        if self.status is not SchedulerStatus.RUNNING:
            raise RuntimeError("Scheduler has not been initialised! Call init")
        LOGGER.debug("Scheduler starting up...")

        while self.status is SchedulerStatus.RUNNING:
            LOGGER.info('Time on Scheduler: {0}'.format(self.env.now))
            if self.buffer.has_observations_ready_for_processing():
                obs = self.buffer.next_observation_for_processing()
                if obs not in self.observation_queue:
                    self.observation_queue.append(obs)
                    ret = self.env.process(self.allocate_tasks(obs))

            if len(self.observation_queue) == 0 \
                    and self.status == SchedulerStatus.SHUTDOWN:
                LOGGER.debug("No more waiting workflows")
                break

            LOGGER.debug("Scheduler Status: %s", self.status)
            yield self.env.timeout(TIMESTEP)

    def is_idle(self):
        """
        Determine if the scheduler has completed its work

        Returns True if idle
        -------
        """

        return len(self.observation_queue) == 0

    def scheduler_status(self):
        """
        The status of the scheduled observation(s) and whether or not the
        scheduler has been delayed yet.

        Returns
        -------
        status: Scheduler.ScheduleStatus
            The status Enum
        """
        return self.schedule_status

    def check_ingest_capacity(self, observation, pipelines, max_ingest):
        """
        Check the cluster and buffer to ensure that we have enough capacity
        to run the INGEST pipeline for the provided observation

        Parameters
        ----------
        observation : core.Telescope.Observation object
            The observation that we are attempting to run/ingest

        pipelines : dict()
            A dictionary of the different types of observations and the
            corresponding pipeline attributes (length, num of machines etc.)

        Returns
        -------
        has_capacity : bool
            True if the buffer AND the cluster have the capacity to run the
            provided observation
            False if either of them do not have capacity.
        """

        buffer_capacity = False
        if self.buffer.check_buffer_capacity(observation):
            LOGGER.debug("Buffer has enough capacity for %s", observation.name)
            buffer_capacity = True

        cluster_capacity = False
        pipeline_demand = pipelines[observation.type]['demand']
        if self.cluster.check_ingest_capacity(pipeline_demand, max_ingest):
            LOGGER.debug(
                "Cluster is able to process ingest for observation %s",
                observation.name
            )
            cluster_capacity = True

        return buffer_capacity and cluster_capacity

    def allocate_ingest(self, observation, pipelines, planner, c='default'):
        """
        Ingest is 'streaming' data to the buffer during the observation
        How we calculate how long it takes remains to be seen

        Parameters
        ---------
        observation : core.Telescope.Observation object
            The observation from which we are starting Ingest

        pipelines : dict
            dictionary storing the different pipeline types supported for the
            current simulation:

            pipelines[observation type][demand]

        Returns
        -------
            True/False

        Raises
        ------
        """
        observation.ast = self.env.now
        self.env.process(
            planner.run(observation, self.buffer)
        )
        pipeline_demand = pipelines[observation.type]['demand']
        self.ingest_observation = observation
        # We do an off-by-one check here, because the first time we run the
        # loop we will be one timestep ahead.
        time_left = observation.duration - 1
        while self.ingest_observation.status is not RunStatus.FINISHED:
            if self.ingest_observation.status is RunStatus.WAITING:
                cluster_ingest = self.env.process(
                    self.cluster.provision_ingest_resources(
                        pipeline_demand,
                        observation
                    )
                )
                ret = self.env.process(
                    self.buffer.ingest_data_stream(
                        observation,
                    )
                )
                self.ingest_observation.status = RunStatus.RUNNING

            elif self.ingest_observation.status is RunStatus.RUNNING:
                if time_left > 0:
                    time_left -= 1
                else:
                    break
            yield self.env.timeout(1)

        if RunStatus.FINISHED:
            self.cluster.clean_up_ingest()

    def print_state(self):
        # Change this to 'workflows scheduled/workflows unscheduled'
        pass

    def allocate_tasks(self, observation, test=False):
        """
        For the current observation, we need to allocate tasks to machines
        based on:

            * The plan that has been generated
            * The result of the scheduler's decision based on the current
            cluster state, and the original plan.
        Returns
        -------

        """
        minst = -1
        current_plan = None
        if observation is None:
            return False
        elif current_plan is None:
            current_plan = observation.plan

        if current_plan is None:
            raise RuntimeError(
                "Observation should have pre-plan; Planner actor has "
                "failed at runtime."
            )
        current_plan.ast = self.env.now
        for task in current_plan.tasks:
            task.workflow_offset = self.env.now
        if current_plan.is_finished():
            if self.buffer.mark_observation_finished(observation):
                current_plan = None
                observation = None

        # Do we have a run-time delay (is our workflow starting later than
        # expected on the cluster? This is the sign of a delay).
        if current_plan.est > self.env.now:
            self.schedule_status.DELAYED
        allocation_triggers = []
        while not test:
            # curr_allocs protects against duplicated scheduled variables
            curr_allocs = []
            machine, task = (None, None)
            status = WorkflowStatus.UNSCHEDULED
            for t in current_plan.tasks:
                if t.task_status is TaskStatus.FINISHED:
                    if t.delay_flag:
                        self.schedule_status = ScheduleStatus.DELAYED
                        self.delay_offset += t.delay_offset
                    current_plan.tasks.remove(t)

            machine, task, status = self.algorithm(
                cluster=self.cluster,
                clock=self.env.now,
                workflow_plan=current_plan
            )
            current_plan.status = status
            if (current_plan.status is WorkflowStatus.DELAYED and
                    self.schedule_status is not WorkflowStatus.DELAYED):
                    self.schedule_status = ScheduleStatus.DELAYED

            if (machine is None and task is None and status is
                    WorkflowStatus.FINISHED):
                if self.buffer.mark_observation_finished(
                        observation
                ):
                    self.observation_queue.remove(observation)
                    break
            elif machine is None or task is None:
                yield self.env.timeout(TIMESTEP)
            else:
                # Run the task on the machie
                if machine not in curr_allocs:
                    if machine.id != task.machine.id:
                        task.update_allocation(machine)
                    ret = self.env.process(
                        self.cluster.allocate_task_to_cluster(task, machine)
                    )
                    LOGGER.info("Allocation {0}-{1} made to cluster".format(
                        task, machine
                    ))
                    allocation_triggers.append(ret)
                    task.task_status = TaskStatus.SCHEDULED
                    curr_allocs.append(machine)
                else:
                    LOGGER.debug(
                        f'Two different tasks have been allocated the '
                        'same machine at the same time. This should '
                        'be avoided in your algorithm!'
                    )
                    continue
                yield self.env.timeout(TIMESTEP)

        yield self.env.timeout(TIMESTEP)

    def to_df(self):
        df = pd.DataFrame()
        df['observation_queue'] = [obs.name for obs in self.observation_queue]
        df['schedule_status'] = [str(self.schedule_status)]
        df['delay_offset'] = [str(self.schedule_status)]
        return df


class SchedulerStatus(Enum):
    SLEEP = 'SLEEP'
    RUNNING = 'RUNNING'
    SHUTDOWN = 'SHUTDOWN'


class ScheduleStatus(Enum):
    ONTIME = 'ONTIME'
    DELAYED = 'DELAYED'
    FAILURE = 'FAILURE'
