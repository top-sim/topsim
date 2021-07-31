import pandas as pd
import logging

logger = logging.getLogger(__name__)

from topsim.core.task import Task, TaskStatus
from topsim.common.globals import TIMESTEP


class Cluster:
    """
    A class used to represent the Cluster, the abstract representation
    of computing resources in the Science Data Processor

    Parameters
    ----------
    machines : dict
        a formatted string to print out what the animal says
    resources : dict
        Dictionary that maintains record of what resources are doing what:
            * 'ingest' resources are being used to process ingest data from
            currently running observation
            * 'occupied' resources are those running workflow tasks
            * 'idle' resources is a dictionary of observations that are
            currently running in batch-processing mode, in which resources
            are provisioned in bulk and then used over the course of the
            workflow. These resources are returned to 'available' at the
            conclusion of the workflow
            * 'available' resources are those that are available for allocation

    tasks : dict
        the sound that the animal makes
    ingest : int
        the number of legs the animal has (default 4)

    Methods
    -------
    says(sound=None)
        Prints the animals name and what sound it makes
    """

    def __init__(self, env, config):

        self.machines, self.system_bandwidth = config.parse_cluster_config()
        # TODO dmachine is a hack, we should improve this
        self.dmachine = {machine.id: machine for machine in self.machines}
        self.cl = ['default']

        self.resources = {
            'ingest': [],
            'occupied': [],
            'idle': {},
            'available': [machine for machine in self.machines],
            'total': len(self.machines)
        }
        self.tasks = {
            'running': [],
            'finished': [],
            'waiting': [],
        }
        self.ingest = {
            'status': False,
            'pipeline': None,
            'observation': None,
            'completed': 0,
            'demand': 0
        }
        self.usage_data = {
            'occupied': 0,
            'ingest': 0,
            'available': len(self.resources['available']),
            'running_tasks': 0,
            'finished_tasks': 0
        }
        self.finished_workflows = []
        self.ingest_pipeline = None
        self.ingest_obervation = None
        self.clusters = {
            'default': {
                'resources': self.resources,
                'tasks': self.tasks,
                'ingest': self.ingest,
                'usage_data': self.usage_data,
                'ingest_pipeline': None,
                'ingest_observation': None,
            }
        }

        self.env = env

    def run(self):
        """
        Start the runtime loop for the cluster, and manage checks on the
        machine

        Returns
        -------

        """
        while True:
            for c in self.cl:
                if not self.clusters[c]['ingest']['status']:
                    self.clusters[c]['usage_data']['ingest'] = 0
                    self.clusters[c]['ingest']['demand'] = 0
                # if self.clusters[c]['tasks']['waiting']:
                #     for task in self.clusters[c]['tasks']['waiting']:
                #         if task.task_status is TaskStatus.FINISHED:
                #             self.clusters[c]['tasks']['waiting'].remove(task)
                #             self.clusters[c]['tasks']['finished'].append(task)
            yield self.env.timeout(1)

    def check_ingest_capacity(self, pipeline_demand, max_ingest_resources,
                              c='default'):
        """
        Check if the Cluster has the machine capacity to process the
        observation Ingest pipeline

        Observation objects have an observation type - this corresponds to
        an ingest pipeline that is set out in the Telescope. This pipeline type
        determines the number of machines in the cluster, and the duration,
        which must be reserved for the observation.

        The cluster also has a maximum number of ingest

        Parameters
        ----------
        pipeline_demand :  int
            The number of machines in the cluster required to run ingest
            pipeline

        max_ingest_resources : int
            The number of resources that may be allowed

        c : str
            cluster
        Returns
        -------
            True if the cluster has capacity
            False if the cluster does not have capacity to run the pipeline
        """

        # Pipeline demand is the number of machines required for the pipeline
        # Length is how long the pipeline will take to
        # ingest/observation will take

        num_available = len(self.clusters[c]['resources']['available'])
        num_ingest = len(self.clusters[c]['resources']['ingest'])

        if pipeline_demand > max_ingest_resources:
            return False

        if len(self.clusters[c]['resources']['available']) >= pipeline_demand \
                and len(
            self.clusters[c]['resources']['ingest']) + pipeline_demand <= \
                max_ingest_resources:
            return True
        else:
            return False

    def provision_ingest_resources(self, demand, observation, c='default'):
        """
        Based on the requirements of the pipeline, provision a certain
        number of resources for ingest

        Parameters
        ----------

        c
        observation
        demand : int
            The type of ingest pipeline - see Observation

        Returns
        -------
        """

        if demand > len(self.current_available_resources()):
            raise RuntimeError(
                f"Failed to check system capacity"
                f" before allocating resources to ingest!"
            )

        tasks = self._generate_ingest_tasks(demand, observation)

        pairs = []

        temp_ingest_resources = (
            self.clusters[c]['resources']['available'][:demand]
        )

        for i, machine in enumerate(temp_ingest_resources):
            pairs.append((machine, tasks[i]))

        self.clusters[c]['ingest']['status'] = True
        self.clusters[c]['ingest']['demand'] = demand
        id = observation.name
        while True:
            for pair in pairs:
                (machine, task) = pair
                ret = self.env.process(
                    self.allocate_task_to_cluster(
                        task, machine, observation=id, ingest=True
                    )
                )
            else:
                break
        yield self.env.timeout(TIMESTEP)

    def clean_up_ingest(self, c='default'):
        """
        Once we finished 'provision ingest', we want to update the cluster
        status before starting the new timestep

        Returns
        -------

        """
        self.clusters[c]['ingest']['completed'] += 1
        self.clusters[c]['ingest']['status'] = False

    def current_available_resources(self):
        """

        Returns
        -------

        """
        return [x for x in self.clusters['default']['resources']['available']]

    def allocate_task_to_cluster(
            self, task, machine,
            predecessor_allocations=None, observation=None, ingest=False,
            c='default'
    ):
        """
        Receive task from scheduler for allocation to specified machine

        Parameters
        ----------
        task :
        machine :
        predecessor_allocations
        ingest
        observation

        pred : list of predecessors machine allocations, for use if the task
        is allocated to a different machine.

        Returns
        -------
        True if task successfully completed
        """

        ret = None

        while True:
            # TODO MOVE THIS CHECK BEFORE WHILE
            if task not in self.clusters[c]['tasks']['running']:
                if (machine not in
                        self.clusters[c]['resources']['available'] and
                        machine not in self.clusters[c]['resources']['ingest']
                        and machine not in self.get_idle_resources(observation)
                ):
                    raise RuntimeError
                if ingest:
                    # Ingest resources are allocated in bulk, so we do that
                    # elsewhere
                    self.clusters[c]['tasks']['running'].append(task)
                    self.clusters[c]['resources']['ingest'].append(machine)
                    self.clusters[c]['resources']['available'].remove(machine)
                    self.clusters[c]['usage_data']['available'] -= 1
                    self.clusters[c]['usage_data']['running_tasks'] += 1
                    task.task_status = TaskStatus.SCHEDULED
                    ret = self.env.process(task.do_work(self.env, machine,
                                                        predecessor_allocations))
                else:
                    # self.clusters[c]['resources']['available'].remove(machine)
                    # self.clusters[c]['resources']['occupied'].append(machine)
                    self._set_machine_occupied(machine, observation)
                    self.clusters[c]['tasks']['running'].append(task)
                    self.clusters[c]['usage_data']['available'] -= 1
                    self.clusters[c]['usage_data']['running_tasks'] += 1

                    task.task_status = TaskStatus.SCHEDULED
                    ret = self.env.process(task.do_work(self.env, machine,
                                                        predecessor_allocations))
                    yield self.env.timeout(1)
            if ret.triggered:
                # machine.stop_task(task)
                self.clusters[c]['tasks']['running'].remove(task)
                self.clusters[c]['usage_data']['running_tasks'] -= 1
                self.clusters[c]['tasks']['finished'].append(task)
                self.clusters[c]['usage_data']['finished_tasks'] += 1
                if ingest:
                    self.clusters[c]['resources']['ingest'].remove(machine)
                else:
                    # self.clusters[c]['resources']['occupied'].remove(machine)
                    self._set_machine_available(machine, observation)
                self.clusters[c]['resources']['available'].append(machine)
                # self.clusters[c]['usage_data']['available'] += 1
                task.task_status = TaskStatus.FINISHED
                task.delay_flag = task.delay_flag
                return task.task_status
            else:
                yield self.env.timeout(TIMESTEP)

    def is_idle(self):
        """
        Check to see if anything is running on the cluster

        This is a way of determining if we are able to finish the simulation.

        Returns
        -------

        """

        no_tasks_running = (
                (len(self.clusters['default']['tasks']['running']) == 0) or
                (len(self.clusters['default']['tasks']['waiting']) == 0)
        )

        no_resources_occupied = (
                (len(self.clusters['default']['resources']['occupied']) == 0) or
                (len(self.clusters['default']['resources']['ingest']) == 0)
        )
        if no_tasks_running and no_resources_occupied:
            return True
        else:
            return False

    def is_occupied(self, machine, observation=None, c='default'):
        """
        Check if the machine is occupied
        Parameters
        ----------
        machine : topsim.core.machine.Machine
            The machine with which we are concerned.

        observation : topsim.core.Observation
            (option) The observation that is associated with the current
            check. This is useful for ensuring machines are not reserved for
            a batch-processing

        c : object
            The identifier for the cluster that is being accessed (in the
            event of multiple clusters). Access the 'default' cluster by
            default (nothing needs changing in this scenario, in which only
            one cluster is specified).
        Returns
        -------

        """
        # TODO add 'is provisioned' check here

        return (machine in self.clusters[c]['resources']['occupied']
                or machine in self.clusters[c]['resources']['ingest'])

    def provision_batch_resources(self, size, name, c='default'):
        """
        Mark a machine on the cluster as being allocated to a workflow, without
        the allocation place just yet.

        Parameters
        ----------
        size: int
            The number of resources to be provisioned based on the
            observation workflow
        name : the observation that is associated with the provisioning
        c :  str
            The name of the cluster (defaults to 'default' if we are only
            using one).

        Returns
        -------
        """
        available_resources = self.get_available_resources()

        tmp = len(available_resources)
        if size > tmp > 0:
            size = tmp
        for m in range(0, size):
            self._add_idle_resource(name, available_resources[0])
        return True

    def release_batch_resources(self, observation, c='default'):
        """
        For a given observation, release these observations from the
        idle-resources section and add them to the available resources 'pile'

        Parameters
        ----------
        observation

        Returns
        -------
        """
        if observation in self.clusters[c]['resources']['idle']:
            self._update_available_resources(observation)
            self._reset_idle_resources(observation)

    def _update_available_resources(self, observation, c='default'):
        """
        De-allacote resources to a given observation (batch-reservation) and
        add them to the 'available' pool of resources.

        Parameters
        ----------
        machine

        Returns
        -------

        """
        idle_resources = self.get_idle_resources(observation)
        for m in idle_resources:
            self.clusters[c]['resources']['available'].append(m)

    def _remove_available_resource(self, machine, c='default'):
        pass

    def get_available_resources(self, c='default'):
        return self.clusters[c]['resources']['available']

    def get_idle_resources(self, observation, c='default'):
        """
        List the resources that are currently provisioned for the
        specified observation.

        Parameters
        ----------
        observation : :py:obj:
        c

        Returns
        -------

        """
        if observation in self.clusters[c]['resources']['idle']:
            return [x for x in
                    self.clusters[c]['resources']['idle'][observation]
                    ]
        else:
            return []

    def get_finished_tasks(self, c='default'):
        return self.clusters[c]['tasks']['finished']

    def _set_machine_occupied(self, machine, observation, c='default'):
        """

        Parameters
        ----------
        machine
        observation

        Returns
        -------

        """
        if machine in self.get_available_resources():
            self.clusters[c]['resources']['available'].remove(machine)
            self.clusters[c]['resources']['occupied'].append(machine)
            return True
        elif observation in self.clusters[c]['resources']['idle']:
            self.clusters[c]['resources']['idle'][observation].remove(machine)
            self.clusters[c]['resources']['occupied'].append(machine)
            return True
        else:
            return False

    def _set_machine_available(self, machine, observation, c='default'):
        """
        Take a machine marked as 'occupied' on the cluster and mark it either
        as available or return it to an observation's pool of resources.
        Parameters
        ----------
        observation

        Returns
        -------

        """
        self.clusters[c]['resources']['occupied'].remove(machine)
        if observation in self.clusters[c]['resources']['idle']:
            self.clusters[c]['resources']['idle'][observation].append(machine)
        else:
            self.clusters[c]['resources']['available'].append(machine)

    def _clean_up_finished_task(self, task, machine, observation):
        pass

    # TODO
    # def _set_task_running(self, task, c='default'):
    #
    #     return self.clusters[c]['tasks']['running'].append(task)

    def _add_idle_resource(self, observation, machine, c='default'):
        """
        Add a resource to the dictionary of idle batch resources for the
        current observation

        Parameters
        ----------
        observation
        machine
        c

        Returns
        -------

        """
        if observation not in self.clusters[c]['resources']['idle']:
            self.clusters[c]['resources']['idle'][observation] = []
        if machine in self.get_available_resources():
            self.clusters[c]['resources']['idle'][observation].append(machine)
            self._remove_available_resource(machine)
        else:
            raise RuntimeError(
                'Attempting to provision resources on machine that is not '
                'available'
            )

    def _get_batch_observations(self, c='default'):
        return list(self.clusters[c]['resources']['idle'].keys())

    def _reset_idle_resources(self, observation, c='default'):
        """
        Remove observation from idle resource dictionary.

        This clears the machine allocations from the idle resources

        Parameters
        ----------
        observation
        c

        Returns
        -------

        """
        self.clusters[c]['resources']['idle'][observation] = []
        return None

    def _remove_available_resource(self, machine, c='default'):
        """

        Parameters
        ----------
        machine
        c

        Returns
        -------

        """
        self.clusters[c]['resources']['available'].remove(machine)

    def _set_machine_task_occupied(self):
        """
        Update allocation dictionaries and output data dictionaries
        Returns
        -------

        """

        pass

    def _set_machine_task_available(self):
        """
        Update allocation dictionaries and output data dictionaries

        Returns
        -------

        """
        pass

    def _set_machine_task_ingest(self):
        """
        Update allocation dictionaries to indicate machine is being used for
        ingest

        Returns
        -------

        """

    def _generate_ingest_tasks(self, demand, observation):
        """
        Parameters
        ----------
        demand : int
            Number of machines that are provisioned for ingest
        duration : int
            Duration of observation (in simulation timesteps)

        Returns
        -------
        tasks : list()
            List of core.Planner.Task objects
        """
        tasks = []
        for i in range(demand):
            t = Task(
                f"{observation.name}_ingest_t{i}",
                0, 0, None, None, 0, 0, 0, None
            )

            t.duration = observation.duration
            t.task_status = TaskStatus.SCHEDULED
            tasks.append(t)
        return tasks

    def to_df(self):
        """

        Notes
        -----
        Currently only works for single resource

        Returns
        -------

        """
        df = pd.DataFrame()
        df['available_resources'] = [
            self.clusters['default']['usage_data']['available']
        ]
        df['occupied_resources'] = [
            self.clusters['default']['usage_data']['occupied']]
        df['ingest_resources'] = [
            self.clusters['default']['usage_data']['ingest']
        ]
        df['running_tasks'] = [
            self.clusters['default']['usage_data']['running_tasks']]
        df['finished_tasks'] = [
            self.clusters['default']['usage_data']['finished_tasks']]
        # df['waiting_tasks'] = [len(self.tasks['waiting'])]

        return df

    def finished_task_time_data(self):
        """
        Each task in the 'finished' component of 'self.clusters' has the
        estimated start times, end times, and the actual start and finish
        times. We can use this to track the expected finish time across the
        two
        Returns
        -------
        """

        finished_tasks = self.clusters['default']['tasks']['finished']
        task_data = {}
        for task in finished_tasks:
            task_data[task.id] = {}
            task_data[task.id]['est'] = task.est + task.workflow_offset
            task_data[task.id]['eft'] = task.eft + task.workflow_offset
            task_data[task.id]['ast'] = task.ast
            task_data[task.id]['aft'] = task.aft
            task_data[task.id]['workflow_offset'] = task.workflow_offset
            task_data[task.id]['observation_id'] = task.id.split('_')[0]
            # task_data['pred'] = [pred for pred in task.pred]

        return pd.DataFrame(task_data)

    def print_state(self):
        # self.clusters[c]['resources']
        self.tasks
        self.ingest

        return {
            'machines': [machine.print_state() for machine in self.machines],
            # 'resources': repr(self.clusters[c]['resources']),
            'tasks': repr(self.tasks),
            # 'ingest': repr(self.clusters[c]['ingest'])
        }
