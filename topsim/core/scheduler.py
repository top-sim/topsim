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
from tqdm import tqdm

from topsim.common.globals import TIMESTEP
from topsim.core.instrument import RunStatus
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

LOGGER = logging.getLogger(__name__)


class Scheduler:
    """

    Attributes
    ----------
    """

    def __init__(self, env, buffer, cluster, planner, algorithm):
        """
        Parameters
        ----------
        env : Simpy.Environment object
            The simulation Environment

        buffer : core.buffer.Buffer object
            The SDP buffer used in the simulation

        cluster : core.cluster.Cluster object
            The Cluster instance used for the simluation

        algorithm : core.cluster.Algorithm object
            The algorithm model using
        """
        self.env = env
        self.algorithm = algorithm
        self.cluster = cluster
        self.buffer = buffer
        self.planner = planner
        self.status = SchedulerStatus.SLEEP
        self.ingest_observation = None
        self.provision_ingest = 0
        self.observation_queue = []
        self.schedule_status = ScheduleStatus.ONTIME
        self.events = []
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
            self.events = []
            if self.env.now % 1000 == 0:
                LOGGER.debug('Time on Scheduler: {0}'.format(self.env.now))
                LOGGER.debug("Scheduler Status: %s", self.status)
            if self.buffer.has_observations_ready_for_processing():
                obs = self.buffer.next_observation_for_processing()
                if obs not in self.observation_queue:
                    self.observation_queue.append(obs)
                    ret = self.env.process(self.allocate_tasks(obs))
                    self._add_event(obs, "queue", "added")
            if (
                    not self.observation_queue and self.status ==
                    SchedulerStatus.SHUTDOWN):
                LOGGER.debug("No more waiting workflows")
                break
            # for obs in self.observation_queue:
            #     if obs.workflow_plan.status =f= WorkflowStatus.FINISHED:
            #         self.cluster.release_batch_resources(obs)
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
                    observation.name)
            else:
                LOGGER.debug('pipeline demand %s + provision_ingest %s',
                             pipeline_demand, self.provision_ingest)
                LOGGER.debug('Cluster is unable to process ingest as two'
                             'observations are scheduled at the same time')

        return buffer_capacity and cluster_capacity

    def allocate_ingest(self, observation, pipelines, planner, max_ingest=None,
                        c='default'):
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
        # self.env.process(
        # observation.plan = planner.run(observation, self.buffer)
        # )

        pipeline_demand = pipelines[observation.name]['ingest_demand']
        ingest_observation = observation
        # We do an off-by-one check here, because the first time we run the
        # loop we will be one timestep ahead.
        time_left = observation.duration - 1
        while ingest_observation.status is not RunStatus.FINISHED:
            if ingest_observation.status is RunStatus.WAITING:
                cluster_ingest = self.env.process(
                    self.cluster.provision_ingest_resources(pipeline_demand,
                        observation))
                ret = self.env.process(
                    self.buffer.ingest_data_stream(observation, ))
                ingest_observation.status = RunStatus.RUNNING

            elif ingest_observation.status is RunStatus.RUNNING:
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

    def allocate_tasks(self, observation):
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

        # TODO determine how to calculate runtime delay with planner being triggered
        # in a new place

        # Answers the question: Do we have a runtime delay?
        # if current_plan.est >= self.env.now + TIMESTEP: # Give us leeway on if we are one timestep out
        #     self.schedule_status.DELAYED

        schedule = {}
        allocation_pairs = {}
        task_pool = set()
        _total_tasks = 0 # len(current_plan.tasks)
        _curr_tasks = 0 # len(current_plan.tasks)
        _tqdm = True
        pbar_setup = False
        pbar = None
        self._add_event(observation, "allocation", "started")
        while True:
            if current_plan:
                current_plan.tasks = self._update_current_plan(current_plan)
            (current_plan,
             schedule,
             task_pool,
             finished) = self._generate_current_schedule(observation,
                                                         current_plan,
                                                         schedule,
                                                         task_pool)
            observation.plan = current_plan
            if not current_plan:
                yield self.env.timeout(TIMESTEP)
                continue
            if _tqdm and not pbar_setup:
                _total_tasks = len(current_plan.tasks)
                _curr_tasks = len(current_plan.tasks)
                pbar = tqdm(total=_total_tasks,
                            desc=f'Scheduler: {observation.name}', unit="Tasks",
                            leave=True, ncols=0)
                pbar_setup = True
            # prev_tasks = _curr_tasks
            # _curr_tasks = len(current_plan.tasks)
            tmp = _curr_tasks
            _curr_tasks = len(current_plan.tasks)
            _nupdate = tmp - _curr_tasks
            if pbar:
                pbar.update(_nupdate)

            if finished:
                # We have finished this observation
                # LOGGER.info(f'{observation.name} Removed from Queue @'
                #             f'{self.env.now}')
                # self.cluster.release_batch_resources(observation)
                break
            # If there are no allocations made this timestep
            elif not schedule:
                LOGGER.debug('No new schedule at time %s', self.env.now)
                yield self.env.timeout(TIMESTEP)
            else:
                # This is where allocations are made to the cluster
                schedule, allocation_pairs = self._process_current_schedule(
                    schedule, allocation_pairs, current_plan.id)
                yield self.env.timeout(TIMESTEP)
        if pbar:
            pbar.close()
        yield self.env.timeout(TIMESTEP)

    def _generate_current_schedule(self, observation, current_plan, schedule,
                                   task_pool):
        """
        Each timestep, we want to generate a schedule based on the observation
        plan and an existing schedule.
        Parameters
        ----------
        current_plan
        schedule

        Returns
        -------
        current_plan, schedule, finished
        """

        finished = False
        nm = f'{observation.name}-algtime'
        self.algtime[nm] = time.time()
        schedule, workflow_plan, task_pool = self.algorithm.run(
            cluster=self.cluster,
            planner=self.planner,
            clock=self.env.now,
            workflow_plan=current_plan,
            existing_schedule=schedule,
            task_pool=task_pool,
            observation=observation)
        self.algtime[nm] = (time.time() - self.algtime[nm])

        if not workflow_plan:
            return current_plan, schedule, task_pool, finished
        else:
            current_plan = workflow_plan

        current_plan.status = workflow_plan.status
        if (
                current_plan.status is WorkflowStatus.DELAYED and
                self.schedule_status is not WorkflowStatus.DELAYED):
            self.schedule_status = ScheduleStatus.DELAYED

        # If the workflow is finished
        if not schedule and workflow_plan.status is WorkflowStatus.FINISHED:
            self._add_event(observation, "allocation", "stopped")
            if self.buffer.mark_observation_finished(observation):
                self.cluster.release_batch_resources(observation.name)
                LOGGER.debug(f'{observation.name} resources released')
                self.observation_queue.remove(observation)
                self._add_event(observation, "queue", "removed")
                finished = True
                LOGGER.info(f"{observation.name} finished @ {self.env.now}")

        return current_plan, schedule, task_pool, finished

    def _process_current_schedule(self, schedule, allocation_pairs,
                                  workflow_id):
        """
        Given a schedule and existing allocations, run through the schedule
        and run the allocation for that tasks if possible

        Parameters
        ----------
        schedule
        allocation_pairs
        workflow_id : The ID of the workflow. This is so in the cluster we
        can find the appropriate set of provisioned resources.

        Returns
        -------

        """
        sorted_tasks = sorted(schedule.keys(), key=lambda t: t.est)
        curr_allocs = []
        # Allocate tasks
        for task in sorted_tasks:
            machine = schedule[task]
            if machine.id != task.allocated_machine_id:
                task.update_allocation(machine)
            # Schedule
            if machine in curr_allocs or self.cluster.is_occupied(machine):
                LOGGER.debug(
                    "Allocation not made to cluster due to double-allocation")
            else:
                allocation_pairs[task.id] = (task, machine)
                pred_allocations = self._find_pred_allocations(task, machine,
                    allocation_pairs)
                if task.task_status != TaskStatus.UNSCHEDULED:
                    raise RuntimeError("Producing schedule with Scheduled "
                                       "Tasks")
                self.env.process(
                    self.cluster.allocate_task_to_cluster(task, machine,
                        pred_allocations, workflow_id))

                LOGGER.debug(f"Allocation {task}-{machine} made to cluster")
                task.task_status = TaskStatus.SCHEDULED
                curr_allocs.append(machine)
                schedule.pop(task, None)

        return schedule, allocation_pairs

    def _update_current_plan(self, current_plan):
        """
        Check the status of tasks in the workflow plan and remove them if
        they are complete

        Each task has a delay_flag that is triggered if the duration or
        finish time is not the same as what was estimated in the planning.
        The method will update the self.schedule_status and self.delay_offset
        class attributes.

        Parameters
        ----------
        current_plan : core.planning.WorkflowPlan
            The workflow plan for an observation in self.observation_queue


        Returns
        -------
        remaining_tasks : list
            List of remaining tasks in the workflow plan
        """

        remaining_tasks = []
        # if not current_plan:
        #     return remaining_tasks
        for t in current_plan.tasks:
            if t.task_status is not TaskStatus.FINISHED:
                remaining_tasks.append(t)
            else:
                if t.delay_flag:
                    self.schedule_status = ScheduleStatus.DELAYED
                    self.delay_offset += t.delay_offset
        return remaining_tasks

    def _find_pred_allocations(self, task, machine, allocations):
        """
        Return a list of machines that the current tasks' predecessors were
        allocated to.

        This is necessary for the Task when determining its duration;
        communication time of data from tasks on other machines will
        be non-negligible and this must be completed in full
        before the task can start.

        Parameters
        ----------
        task

        Returns
        -------

        """
        pred_allocations = []
        for pred in task.pred:
            pred_task, pred_machine = allocations[pred]
            if pred_machine != machine:
                alt = True
                pred_allocations.append(pred_task)
        return pred_allocations

    def to_df(self):
        """
        Convert scheduling timestep data into dataframe for the
        :py:obj:`~topsim.core.monitor.Monitor` actor.

        Returns
        -------
        df : pandas.DataFrame
            Dataframe object with all the relevant data.

        """
        df = pd.DataFrame()
        df['scheduler_observation_queue'] = [int(len(self.observation_queue))]
        df['schedule_status'] = [str(self.schedule_status.value)]
        df['delay_offset'] = [self.delay_offset]
        tmp = f'alg'
        if self.algtime:
            for key, value in self.algtime.items():
                df[key] = value
        return df

    def to_summary(self):
        """
        Generate summary information when a key event occurs during the
        current timestep

        The scheduler has the following key events that can happen each timestep

        *

        Returns
        -------
        summary : dict
            list of key-value pairs with the 'event': 'event_name'
        """

        return self.events

    def _add_event(self, observation, resource, event):
        self.events.append({"time": int(self.env.now), "actor": "scheduler",
                            "observation": observation.name, "event": event,
                            "resource": resource})


class SchedulerStatus(Enum):
    """
    The status of the Scheduler Actor

    Used to determine if the Scheduler is running. If it is not running,
    and is marked as SHUTDOWN, then this is one of the exit conditions for
    a simulation.
    """
    SLEEP = 'SLEEP'
    RUNNING = 'RUNNING'
    SHUTDOWN = 'SHUTDOWN'


# TODO Update to WorkflowStatus to avoid SchedulerStatus single-letter typos
class ScheduleStatus(Enum):
    """
    # TODO docstring
    """
    ONTIME = 'ONTIME'
    DELAYED = 'DELAYED'
    FAILURE = 'FAILURE'


class SchedulerEvent(Enum):
    """
    When an event occurs, send information to the monitor with the event
    information
    """
