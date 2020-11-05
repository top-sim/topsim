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
from enum import Enum
from topsim.core.telescope import RunStatus
from topsim.core.planner import WorkflowStatus
from topsim.core.task import TaskStatus

logger = logging.getLogger(__name__)


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
        self.waiting_observations = []
        self.current_observation = None
        self.cluster = cluster
        self.buffer = buffer
        self.current_plan = None
        self.status = SchedulerStatus.SLEEP
        self.ingest_observation = None

    def run(self):
        if self.status is not SchedulerStatus.RUNNING:
            raise RuntimeError("Scheduler has not been initialised! Call init")
        logger.debug("Scheduler starting up...")
        while self.status is SchedulerStatus.RUNNING:
            # AT THE END OF THE SIMULATION, WE GET STUCK HERE. NEED TO EXIT
            if self.buffer.workflow_ready_observations \
                    or self.waiting_observations:

                for observation in self.buffer.waiting_observation_list:
                    self.buffer.request_data_from(observation)
                    if observation not in self.waiting_observations:
                        self.waiting_observations.append(observation)

                allocation = self.allocate_tasks()

                if allocation:
                    logger.info("Successfully allocated")

            if len(self.waiting_observations) == 0 \
                    and self.status == SchedulerStatus.SHUTDOWN:
                logger.debug("No more waiting workflows")
                break

            logger.debug("Scheduler Status: %s", self.status)
            yield self.env.timeout(1)

    def start_ingest_pipelines(self, observation, pipeline):

        streaming_time = int(observation.duration / 2)
        if self.buffer.check_buffer_capacity(observation.project_output):
            yield self.env.timeout(streaming_time)

        # TODO Buffer should run at beginning of simulation, and we should
        #  add runtime checks for the buffer to mimic this functionality.
        # in fact, we don't even take observation as a parameter anymore. 
        yield self.env.process(self.buffer.run(observation))

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
            logger.debug("Buffer has enough capacity for %s", observation.name)
            buffer_capacity = True

        cluster_capacity = False
        pipeline_demand = pipelines[observation.type]['demand']
        if self.cluster.check_ingest_capacity(pipeline_demand, max_ingest):
            logger.debug(
                "Cluster is able to process ingest for observation %s",
                observation.name
            )
            cluster_capacity = True

        return buffer_capacity and cluster_capacity

    def allocate_ingest(self, observation, pipelines):
        """
        Ingest is 'streaming' data to the buffer during the observation
        How we calculate how long it takes remains to be seen
        For the time being, we will be doubling the observation time

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
                        observation.duration
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
                    self.ingest_observation.run_status = RunStatus.FINISHED
                    break
            yield self.env.timeout(1)


    # def add_workflow(self, workflow):
    # 	print("Adding", workflow, "to workflows")
    # 	self.observations_for_processing.append(workflow)
    # 	print("Waiting workflows", self.observations_for_processing

    def init(self):
        self.status = SchedulerStatus.RUNNING
        return self.status

    def shutdown(self):
        self.status = SchedulerStatus.SHUTDOWN
        return self.status

    def print_state(self):
        # Change this to 'workflows scheduled/workflows unscheduled'
        return {
            'observations_for_processing': [observation.plan.id for observation
                                            in self.waiting_observations]
        }

    # TODO ALLOCATE TASKS NEEDS A CHANGE. THere is too much code in one function,
    #  we need to split this up more.

    def allocate_tasks(self):
        """
        Allocate tasks calls a number of methods to allocate tasks according to
        the self.algorithm attribute. This takes the current simulation clock time
        (env.now), the cluster, and the workflow plan as a parameter and
        returns an allocation if necessary.

        This function returns the current progress of the workflows on the
        cluster as a list. This list is then passed to the Telescope to
        help with it's observation decisions.
        Returns
        -------

        """
        logger.debug('Attempting to schedule workflow to cluster')

        # Min -scheduling time
        minst = -1
        # TODO new method 'check_for_ready_observations)

        if not self.current_plan:
            for observation in self.waiting_observations:
                st = observation.plan.start_time
                if minst == -1 or st < minst:
                    minst = st
                    self.current_plan = observation.plan
                    self.current_plan.start_time = self.env.now
                    self.current_observation = observation
                    logger.info(
                        "New observation %s scheduled for processing @ Time: "
                        "%s", observation.plan.id, self.env.now
                    )

        # TODO new method 'clean_up_processed_workflow'

        if self.current_plan.status is WorkflowStatus.FINISHED:
            # TODO wrap this into a function moving forward
            self.waiting_observations.remove(self.current_observation)
            self.buffer.request_data_from(self.current_observation)
            self.buffer.waiting_observation_list.remove(
                self.current_observation)
            logger.info('%s finished processing @ %s',
                        self.current_observation.name, self.env.now)
            self.current_plan = None
            self.current_observation = None

        if self.waiting_observations:
            for observation in self.waiting_observations:
                if observation.plan.status is WorkflowStatus.FINISHED:
                    self.waiting_observations.remove(self.current_observation)
                    self.buffer.waiting_observation_list.remove(
                        self.current_observation
                    )
            logger.debug(
                "Currently waiting to process: %s", self.waiting_observations
            )
        else:
            logger.debug("Nothing in Buffer to process")

        if self.current_plan:
            logger.debug(
                "Current plan: %s, %s ",
                self.current_plan.id,
                self.current_plan.exec_order
            )

            while True:
                machine, task, status = self.algorithm(
                    self.cluster, self.env.now, self.current_plan
                )
                if machine is None or task is None:
                    break
                else:
                    # Runs the task on the machie
                    # task.machine = machine
                    task.task_status = TaskStatus.SCHEDULED

                    machine.run(task)
                    if task.task_status is TaskStatus.SCHEDULED:
                        self.cluster.running_tasks.append(task)

        return True

    def find_appropriate_machine_in_cluster(self, machine_id):
        for machine in self.cluster.machines:
            if machine.id == machine_id:
                return machine


class SchedulerStatus(Enum):
    SLEEP = 'SLEEP'
    RUNNING = 'RUNNING'
    SHUTDOWN = 'SHUTDOWN'
