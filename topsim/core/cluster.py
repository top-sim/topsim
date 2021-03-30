import pandas as pd

from topsim.core.task import Task, TaskStatus
from topsim.common.globals import TIMESTEP


class Cluster:
    """
    A class used to represent the Cluster, the abstract representation
    of computing resources in the Science Data Processor

    Attributes
    ----------
    machines : dict
        a formatted string to print out what the animal says
    resources : dict
        the name of the animal
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
        """
        :param env:
        :param spec:
        """
        self.machines, self.system_bandwidth = config.parse_cluster_config()
        self.dmachine = {machine.id: machine for machine in self.machines}
        self.cl = ['default']
        self.resources = {
            'ingest': [],
            'occupied': [],
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
        while True:
            for c in self.cl:
                if not self.clusters[c]['ingest']['status']:
                    self.clusters[c]['usage_data']['ingest'] = 0
                    self.clusters[c]['ingest']['demand'] = 0
                if self.clusters[c]['tasks']['waiting']:
                    for task in self.clusters[c]['tasks']['waiting']:
                        if task.task_status is TaskStatus.FINISHED:
                            self.clusters[c]['tasks']['waiting'].remove(task)
                            self.clusters[c]['tasks']['finished'].append(task)
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
            self.clusters[c]['resources']['ingest']) < max_ingest_resources:
            return True
        else:
            return False

    def provision_ingest_resources(self, demand, observation, c='default'):
        """
        Based on the requirements of the pipeline, provision a certain
        number of resources for ingest

        Parameters
        ----------

        demand : int
            The type of ingest pipeline - see Observation
        duration : int

        Returns
        -------
        """

        tasks = self._generate_ingest_tasks(demand, observation)

        pairs = []
        self.clusters[c]['resources']['ingest'].extend(
            self.clusters[c]['resources']['available'][:demand])
        self.clusters[c]['resources']['available'] = \
            self.clusters[c]['resources']['available'][demand:]

        self.usage_data['available'] = len(
            self.clusters[c]['resources']['available']
        )
        self.usage_data['ingest'] = len(self.clusters[c]['resources']['ingest'])

        for i, machine in enumerate(self.clusters[c]['resources']['ingest']):
            pairs.append((machine, tasks[i]))

        self.clusters[c]['ingest']['status'] = True
        self.clusters[c]['ingest']['demand'] = demand

        self.usage_data['running_tasks'] = self.clusters[c]['ingest']['demand']
        curr_tasks = {}
        while True:
            for pair in pairs:
                (machine, task) = pair
                # if task not in self.tasks['running']:
                #     self.tasks['running'].append(task)
                ret = self.env.process(
                    self.allocate_task_to_cluster(task, machine, ingest=True)
                )
            if len(self.clusters[c]['resources']['ingest']) == 0:
                self.clusters[c]['ingest']['completed'] += 1
                self.clusters[c]['ingest']['status'] = False
                # We've finished ingest
                break
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

    def allocate_task_to_cluster(self, task, machine, ingest=False,
                                 c='default'):
        """
        Receive task from scheduler for allocation to specified machine

        Returns
        -------
        True if task successfully completed
        """
        ret = None

        while True:
            if task not in self.clusters[c]['tasks']['running']:
                self.clusters[c]['tasks']['running'].append(task)
                if not ingest:
                    # Ingest resources are allocated in bulk, so we do that
                    # elsewhere
                    self.clusters[c]['resources']['occupied'].append(machine)
                    self.clusters[c]['resources']['available'].remove(machine)
                    self.clusters[c]['usage_data']['available'] -= 1
                    self.clusters[c]['usage_data']['running_tasks'] += 1
                    duration = round(task.flops / machine.cpu)
                    if duration != task.duration:
                        task.duration = duration

                task.task_status = TaskStatus.SCHEDULED

                # The currently-stored duration may be a result of a
                # preliminary allocation to another machine. Here we make
                # sure we update that.

                # ret = self.env.process(machine.run(task, self.env))
                machine.run_task(task)
                ret = self.env.process(task.do_work(self.env))
                yield self.env.timeout(0)
            if ret.triggered:
                machine.stop_task(task)
                self.clusters[c]['tasks']['running'].remove(task)
                self.clusters[c]['usage_data']['running_tasks'] -= 1
                self.clusters[c]['tasks']['finished'].append(task)
                self.clusters[c]['usage_data']['finished_tasks'] += 1
                if ingest:
                    self.clusters[c]['resources']['ingest'].remove(machine)
                else:
                    self.clusters[c]['resources']['occupied'].remove(machine)
                self.clusters[c]['resources']['available'].append(machine)
                self.clusters[c]['usage_data']['available'] += 1
                task.task_status = TaskStatus.FINISHED
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

    def is_occupied(self, machine, c='default'):
        """
        Check if the machine is occupied
        Parameters
        ----------
        machine : topsim.core.machine.Machine
            The machine with which we are concerned.
        c : object
            The identifier for the cluster that is being accessed (in the
            event of multiple clusters). Access the 'default' cluster by
            default (nothing needs changing in this scenario, in which only
            one cluster is specified).
        Returns
        -------

        """
        return (machine in self.clusters[c]['resources']['occupied']
                or machine in self.clusters[c]['resources']['ingest'])

    # TODO
    def find_unnoccupied_resources(self, task_reqs):
        """
        Return a list of unnoccupied machines that have the capacity based
        on the task requirements. Task requirements will be any combination
        of machine resources (FLOPs, IO, Memory).

        This is intended to be used by a scheduling Actor in the event that
        a resource identified in a WorkflowPlan is no longer available.

        Parameters
        ----------
        task_reqs : dict
            Dictionary of required resources for a given task.

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
                f"ingest-t{i}-{observation.name}",
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
