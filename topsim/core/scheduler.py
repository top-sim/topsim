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
import time
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
        self.provision_ingest = 0
        self.observation_queue = []
        self.schedule_status = ScheduleStatus.ONTIME
        self.algtime = {}
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
            LOGGER.debug('Time on Scheduler: {0}'.format(self.env.now))
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
        pipeline_demand = pipelines[observation.name]['ingest_demand']
        if self.cluster.check_ingest_capacity(pipeline_demand, max_ingest):
            if self.provision_ingest + pipeline_demand <= max_ingest:
                cluster_capacity = True
                self.provision_ingest += pipeline_demand
                LOGGER.debug(
                    "Cluster is able to process ingest for observation %s",
                    observation.name
                )
            else:
                LOGGER.debug('Cluster is unable to process ingest as two'
                             'observations are scheduled at the same time')

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
        pipeline_demand = pipelines[observation.name]['ingest_demand']
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
            self.provision_ingest -= pipeline_demand
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

        # Do we have a runtime delay?
        if current_plan.est > self.env.now:
            self.schedule_status.DELAYED

        existing_schedule = set()
        allocation_pairs = {}
        while not test:
            # curr_allocs protects against duplicated scheduled variables
            # machine, task = (None, None)
            status = WorkflowStatus.UNSCHEDULED
            np = []
            for t in current_plan.tasks:
                if t.task_status is not TaskStatus.FINISHED:
                    np.append(t)
                else:
                    if t.delay_flag:
                        self.schedule_status = ScheduleStatus.DELAYED
                        self.delay_offset += t.delay_offset
            current_plan.tasks = np
            nm = f'{observation.name}-algtime'
            self.algtime[nm] = time.time()
            timestep_allocations, status = self.algorithm(
                cluster=self.cluster,
                clock=self.env.now,
                workflow_plan=current_plan
            )
            LOGGER.info(f'{observation.name} has '
                        f'{len(current_plan.tasks)} tasks @ {self.env.now}')
            self.algtime[nm] = (time.time() - self.algtime[nm])
            current_plan.status = status
            if (current_plan.status is WorkflowStatus.DELAYED and
                    self.schedule_status is not WorkflowStatus.DELAYED):
                self.schedule_status = ScheduleStatus.DELAYED

            existing_schedule.update(timestep_allocations)
            # If the workflow is finished
            if (not existing_schedule and status is
                    WorkflowStatus.FINISHED):
                if self.buffer.mark_observation_finished(
                        observation
                ):
                    self.observation_queue.remove(observation)
                    LOGGER.info(f'{observation.name} Removed from Queue @'
                                f'{self.env.now}')
                    break

            # If there are no allocations made this timestep
            elif not existing_schedule:
                yield self.env.timeout(TIMESTEP)
            else:
                sorted_schedule = sorted(
                    existing_schedule, key=lambda a:  a[1].est
                )
                curr_allocs = []
                for allocation in sorted_schedule:
                    machine, task = allocation
                    if machine.id != task.machine:
                        task.update_allocation(machine)
                        task.machine = machine
                    allocation_success = None
                    allocation_pairs[task.id] = (task, machine)
                    alt = False
                    altmachine = []
                    for pred in task.pred:
                        if allocation_pairs[pred][1] != machine:
                            alt = True
                            altmachine.append(allocation_pairs[pred][0])
                    # Schedule
                    if machine in curr_allocs or self.cluster.is_occupied(machine):
                        continue
                    ret = self.env.process(
                        self.cluster.allocate_task_to_cluster(task, machine,
                                                              alt,altmachine)
                    )
                    """
                    `ret` will only have a value if an error occurs;
                    otherwise, we are trying to get value that doesn't
                    exist. Hence, the best case scenario is AttributeError. 
                    """
                    try:
                        allocation_success = ret.value
                    except AttributeError:
                        allocation_success = True
                    if allocation_success:
                        LOGGER.debug("Allocation {0}-{1} made to "
                                 "cluster".format(
                            task, machine
                        ))
                        task.task_status = TaskStatus.SCHEDULED
                        curr_allocs.append(machine)
                        existing_schedule.remove(allocation)
                    else:
                        LOGGER.debug("Allocation was not made to cluster "
                                    "due to double-allocation")
                yield self.env.timeout(TIMESTEP)

        yield self.env.timeout(TIMESTEP)

    def tmp(self, task, machine):
        allocation_pairs = {}
        allocation_pairs[task.id] = machine
        alt_machine = False
        for pred in task.pred:
            if allocation_pairs[pred] != machine:
                alt_machine = True
        if alt_machine:
            self.env.process(task._wait_for_transfer(self.env))

    def to_df(self):
        df = pd.DataFrame()
        queuestr = f''
        for obs in self.observation_queue:
            queuestr += f'{obs.name}'
        df['observation_queue'] =queuestr
        df['schedule_status'] = [str(self.schedule_status)]
        df['delay_offset'] = [str(self.schedule_status)]
        tmp = f'alg'
        if self.algtime:
            for key, value in self.algtime.items():
                df[key] = value
        else:
            df['algtime'] = tmp
        return df


class SchedulerStatus(Enum):
    SLEEP = 'SLEEP'
    RUNNING = 'RUNNING'
    SHUTDOWN = 'SHUTDOWN'


class ScheduleStatus(Enum):
    ONTIME = 'ONTIME'
    DELAYED = 'DELAYED'
    FAILURE = 'FAILURE'
