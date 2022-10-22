import pandas as pd
import logging

from topsim.core.task import Task, TaskStatus
from topsim.common.globals import TIMESTEP

logger = logging.getLogger(__name__)


class Cluster:
    """
    A class used to represent the Cluster, the abstract representation
    of computing resources in the Science Data Processor

    The Cluster runs on a per-timestep capacity through its `run()` function
    in the same way other actors do; however, it's runtime work is minimal.

    The main purpose of the Cluster is to provide access methods for
    requesting machine data (either aggregate or individual), and for
    requesting allocations to schedule. The majority of the Cluster is
    therefore 'read-only' from a user perspective. The only situations in
    which a user will change the system on the cluster is by using a
    non-default resource provisioning policy (see Notes below).



    Parameters
    ----------
    env : :py:obj:`simpy.Environment`
        The environment for the current simulation.

    config : :py:obj:`~topsim.core.config.Config`
        The configuration object for the simulation. See
        :py:obj:`~topsim.core.simulation.Simulation` for more details.

    Notes
    -----
    TopSim defaults to a 'free-for-all' style of resource allocation; unless
    otherwise stated, a resource that is marked as 'available' may be used
    for any task (provided capacity restrictions are met etc.).

    If a SLURM-type resource provisioning approach is wanted, where a portion
    of resources are allocated to a specific workflow for the duration of
    that workflow, it is possible to use the
    :py:meth:`~topsim.core.cluster.Cluster.provision_batch_resources`
    class method in your (online) Scheduling
    algorithm. This associates a set of machines for your workflow based
    on a provisioning scheme of your design.

    The clean-up of resources is completed by the Scheduler once all
    :py:obj:`~topsim.core.task.Task` objects in the
    :py:obj:`~topsim.core.planner.WorkflowPlan` have finished running,
    and requires no additional code on behalf of the user.

    """

    def __init__(self, env, config):
        """
        Initialising a Cluster object requires only the Simpy environment and a
        Config object.

        """
        self.env = env  #: Simulation Environment object
        machines, system_bandwidth = config.parse_cluster_config()
        self.machines = machines
        #: `list` of :py:obj:`~topsim.core.machine.Machine objects`
        self.system_bandwidth = system_bandwidth
        #: System bandwidth across the cluster

        # TODO dmachine is a hack, we should improve this
        self.machine_ids = {machine.id: machine for machine in self.machines}
        self.cl = ['default']

        self._resources = {'ingest': [], 'occupied': [], 'idle': {},
                           'available': [machine for machine in self.machines],
                           'total': len(self.machines)}

        self._tasks = {'running': [], 'finished': {},
                       'waiting': [], }  # Dictionary of tasks on system

        self._ingest = {'status': False, 'pipeline': None, 'observation': None,
                        'completed': 0,
                        'demand': 0}  # Dictionary of current ingest information

        self._usage_data = {'occupied': 0, 'ingest': 0,
                            'available': len(self._resources['available']),
                            'running_tasks': 0,
                            'finished_tasks': 0}  # Data to more easily
        # create output data frame

        self.num_provisioned_obs = 0
        self.events = []
        self._clusters = {
            'default': {'resources': self._resources, 'tasks': self._tasks,
                        'ingest': self._ingest, 'usage_data': self._usage_data,
                        'ingest_pipeline': None, 'ingest_observation': None, }}

    def run(self):
        """
        Start the runtime loop for the cluster, and manage checks on the
        machine



        Yields
        -------
        Standard TIMESTEP timeout for the simulation.
        """
        while True:
            self.events = []
            # Manage each cluster
            for c in self.cl:
                if not self._clusters[c]['ingest']['status']:
                    self._clusters[c]['usage_data']['ingest'] = 0
                    self._clusters[c]['ingest']['demand'] = 0
            yield self.env.timeout(TIMESTEP)

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

        num_available = len(self._clusters[c]['resources']['available'])
        num_ingest = len(self._clusters[c]['resources']['ingest'])

        if pipeline_demand > max_ingest_resources:
            return False

        if len(self._clusters[c]['resources'][
                   'available']) >= pipeline_demand and len(
            self._clusters[c]['resources'][
                'ingest']) + pipeline_demand <= max_ingest_resources:
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
            raise RuntimeError(f"Failed to check system capacity"
                               f" before allocating resources to ingest!")

        tasks = self._generate_ingest_tasks(demand, observation)

        pairs = []

        temp_ingest_resources = (
            self._clusters[c]['resources']['available'][:demand])

        for i, machine in enumerate(temp_ingest_resources):
            pairs.append((machine, tasks[i]))

        # TODO UPDATE HOW WE ALLOCATE TASKS TO RESOURCES HERE SO WE DON'T GENERATE SAME PAIRS
        self._clusters[c]['ingest']['status'] = True
        self._clusters[c]['ingest']['demand'] = demand
        id = observation.name
        while True:
            for pair in pairs:
                (machine, task) = pair
                self._clusters[c]['resources']['ingest'].append(machine)
                self._clusters[c]['resources']['available'].remove(machine)
                ret = self.env.process(
                    self.allocate_task_to_cluster(task, machine, observation=id,
                                                  ingest=True))
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
        self._clusters[c]['ingest']['completed'] += 1
        self._clusters[c]['ingest']['status'] = False

    def current_available_resources(self):
        """
        Produce a list of current available resources

        We use a list comprehension here because otherwise we would
        return an actual reference to the resources, causing issues if we
        want to remove elements from the list wherever we are using it.

        Returns
        -------
        A list that duplicates the entries for current available resources
        """
        return [x for x in self._clusters['default']['resources']['available']]

    def allocate_task_to_cluster(self, task, machine,
                                 predecessor_allocations=None, observation=None,
                                 ingest=False, c='default'):
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
            if task not in self._clusters[c]['tasks']['running']:
                # THIS CHECK DOESN"T WORK FIX IT SOMEHOW
                if (machine not in self._clusters[c]['resources'][
                    'available'] and (machine not in
                        self._clusters[c]['resources'][
                            'ingest'] and machine not in
                        self.get_idle_resources(
                            observation))):
                    raise RuntimeError
                if ingest:
                    # Ingest resources allocated separately from scheduler
                    # self._set_task_running()
                    # self._set_machine_occupied(ingest=True)
                    self._clusters[c]['tasks']['running'].append(task)
                    self._clusters[c]['tasks']['finished'][task] = False
                    # self._clusters[c]['resources']['ingest'].append(machine)
                    # self._clusters[c]['resources']['available'].remove(machine)
                    self._clusters[c]['usage_data']['available'] -= 1
                    self._clusters[c]['usage_data']['running_tasks'] += 1
                    self._clusters[c]['usage_data']['ingest'] += 1
                    task.task_status = TaskStatus.SCHEDULED
                    # ret = self.env.process(task.do_work(self.env, machine,
                    #                                     predecessor_allocations))
                    ret = machine.run(task, self.env, predecessor_allocations)
                else:
                    # self.clusters[c]['resources']['available'].remove(machine)
                    # self.clusters[c]['resources']['occupied'].append(machine)
                    self._set_machine_occupied(machine, observation)
                    self._clusters[c]['tasks']['running'].append(task)
                    self._clusters[c]['usage_data']['available'] -= 1
                    self._clusters[c]['usage_data']['running_tasks'] += 1

                    task.task_status = TaskStatus.SCHEDULED
                    ret = self.env.process(task.do_work(self.env, machine,
                                                        predecessor_allocations))
                    yield self.env.timeout(1)
            if ret.triggered:
                # machine.stop_task(task)
                self._clusters[c]['tasks']['running'].remove(task)
                self._clusters[c]['usage_data']['running_tasks'] -= 1
                # self._clusters[c]['tasks']['finished'].append(task)
                self._clusters[c]['tasks']['finished'][task] = True
                self._clusters[c]['usage_data']['finished_tasks'] += 1
                if ingest:
                    self._clusters[c]['resources']['ingest'].remove(machine)
                    self._clusters[c]['resources']['available'].append(machine)
                    self._clusters[c]['usage_data']['ingest'] -= 1
                else:
                    # self.clusters[c]['resources']['occupied'].remove(machine)
                    self._set_machine_available(machine, observation)
                self._clusters[c]['usage_data']['available'] += 1
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
        True if nothing is running, False otherwise
        """

        no_tasks_running = (
                (len(self._clusters['default']['tasks']['running']) == 0) or (
                len(self._clusters['default']['tasks']['waiting']) == 0))

        no_resources_occupied = ((len(
            self._clusters['default']['resources']['occupied']) == 0) or (len(
            self._clusters['default']['resources']['ingest']) == 0))
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

        return (machine in self._clusters[c]['resources'][
            'occupied'] or machine in self._clusters[c]['resources']['ingest'])

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

        self.num_provisioned_obs += 1
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
        if observation in self._clusters[c]['resources']['idle']:
            self._update_available_resources(observation)
            self._reset_idle_resources(observation)

    def get_machine_from_id(self, id, c='default'):
        """

        Parameters
        ----------
        id : str
            The str-id of the machine we want to access
        c

        Returns
        -------
        :py:obj:`~topsim.core.machine.Machine`
        """
        return self.machine_ids[id]


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
            self._clusters[c]['resources']['available'].append(m)

    def get_available_resources(self, c='default'):
        return self._clusters[c]['resources']['available']

    def is_observation_provisioned(self, observation, c='default'):
        return observation in self._clusters[c]['resources']['idle']

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
        if observation in self._clusters[c]['resources']['idle']:
            return [x for x in
                    self._clusters[c]['resources']['idle'][observation]]
        else:
            return []

    @property
    def finished_tasks(self, c='default'):
        """
        Returns the tasks that are finished on the cluster

        Returns
        -------
        `list` of finished :py:obj:`topsim.core.task.Task` objects
        """
        return [x for x in self._clusters[c]['tasks']['finished']]

    def is_task_finished(self, task, c='default'):
        """
        Check if task is finished or still running on the cluster

        Parameters
        ----------
        task : task object

        Returns
        -------
        True if task is finished (in cluster finished dictionary)
        """
        if task not in self._clusters[c]['tasks']['finished']:
            return False
        else:
            return self._clusters[c]['tasks']['finished'][task]

    def get_finished_tasks(self, c='default'):
        """
        Return a list of the tasks that are finished
        Parameters
        ----------
        c

        Returns
        -------

        """
        return [x for x in self._clusters[c]['tasks']['finished']]

    def _set_machine_occupied(self, machine, observation, ingest=False, c='default'):
        """

        Parameters
        ----------
        machine
        observation
        ingest : bool
            If True, we update which pool we keep track of, as we keep
            keep ingest resources separate from 'occupied' for reporting
            reasons.

        Returns
        -------

        """
        if ingest:
            pool = 'ingest'
        else:
            pool = 'occupied'

        if machine in self.get_available_resources():
            self._clusters[c]['resources']['available'].remove(machine)
            self._clusters[c]['resources'][pool].append(machine)
            return True
        elif observation in self._clusters[c]['resources']['idle']:
            self._clusters[c]['resources']['idle'][observation].remove(machine)
            self._clusters[c]['resources'][pool].append(machine)
            return True
        else:
            return False

    def _set_machine_available(self, machine, observation, ingest=False, c='default'):
        """
        Take a machine marked as 'occupied' on the cluster and mark it either
        as available or return it to an observation's pool of resources.
        Parameters
        ----------
        observation

        Returns
        -------

        """
        if ingest:
            self._clusters[c]['resources']['ingest'].remove(machine)
        else:
            self._clusters[c]['resources']['occupied'].remove(machine)

        # Return to provisioned or unprovisioned resource pool
        if observation in self._clusters[c]['resources']['idle']:
            self._clusters[c]['resources']['idle'][observation].append(machine)
        else:
            self._clusters[c]['resources']['available'].append(machine)

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
        if observation not in self._clusters[c]['resources']['idle']:
            self._clusters[c]['resources']['idle'][observation] = []
        if machine in self.get_available_resources():
            self._clusters[c]['resources']['idle'][observation].append(machine)
            self._remove_available_resource(machine)
        else:
            raise RuntimeError(
                'Attempting to provision resources on machine that is not '
                'available')

    def _get_batch_observations(self, c='default'):
        return list(self._clusters[c]['resources']['idle'].keys())

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
        if self._clusters[c]['resources']['idle'][observation]:
            self._clusters[c]['resources']['idle'].pop(observation)
            self.num_provisioned_obs -= 1
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
        self._clusters[c]['resources']['available'].remove(machine)


    def _add_event(self, observation, resource, event):
        self.events.append(
            {
                "time": self.env.now, "actor": "cluster",
                "observation": observation.name, "event": event,
                "resource": resource
            }
        )

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
            t = Task(f"{observation.name}_ingest_t{i}", 0, 0, None, None, 0, 0,
                     0, None)
            # obseration.start_time
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
            self._clusters['default']['usage_data']['available']]
        df['ingest_resources'] = [
            self._clusters['default']['usage_data']['ingest']]
        df['running_tasks'] = [
            self._clusters['default']['usage_data']['running_tasks']]
        df['finished_tasks'] = [
            self._clusters['default']['usage_data']['finished_tasks']]

        df['provisioned_observations'] = [
            len(self._clusters['default']['resources']['idle'])]
        # df['waiting_tasks'] = [len(self.tasks['waiting'])]

        return df

    def _update_usage_data(self, resource: str, value):
        """
        Update the usage statistics of the resource with the new value
        Parameters
        ----------
        resource : str
            A string value from the 'usage_data' dictionary

        Returns
        -------
        The new value
        """
        self._clusters['default']['usage_data']['resource'] = value
        return value

    def finished_task_time_data(self):
        """
        Each task in the 'finished' component of 'self.clusters' has the
        estimated start times, end times, and the actual start and finish
        times. We can use this to track the expected finish time across the
        two
        Returns
        -------
        """

        finished_tasks = self._clusters['default']['tasks']['finished']
        task_data = {}
        for task in finished_tasks:
            task_data[task.id] = {}
            task_data[task.id]['est'] = task.est + task.workflow_offset
            task_data[task.id]['eft'] = task.eft + task.workflow_offset
            task_data[task.id]['ast'] = task.ast
            task_data[task.id]['aft'] = task.aft
            task_data[task.id]['workflow_offset'] = task.workflow_offset
            task_data[task.id]['observation_id'] = task.id.split('_')[
                0]  # task_data['pred'] = [pred for pred in task.pred]

        return pd.DataFrame(task_data).infer_objects()

    def __len__(self):
        return len(self.machines)
