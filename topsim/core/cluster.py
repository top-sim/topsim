import pandas as pd

from topsim.core import config
from topsim.core.task import Task, TaskStatus


class Cluster:
    """
    A class used to represent the Cluster, the abstract representation
    of computing resources in the Science Data Processor

    Attributes
    ----------
    says_str : str
        a formatted string to print out what the animal says
    name : str
        the name of the animal
    sound : str
        the sound that the animal makes
    num_legs : int
        the number of legs the animal has (default 4)

    Methods
    -------
    says(sound=None)
        Prints the animals name and what sound it makes
    """

    def __init__(self, env, spec):
        """
        :param env:
        :param spec:
        """
        try:
            self.machines, self.system_bandwidth = \
                config.process_machine_config(spec)
        except OSError:
            raise
        self.dmachine = {machine.id: machine for machine in self.machines}

        self.resources = {
            'ingest': [],
            'occupied': [],
            'available': [machine for machine in self.machines]
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
            'available': len(self.resources['available'])
        }
        self.finished_workflows = []
        self.ingest_pipeline = None
        self.ingest_obervation = None
        self.env = env

    def run(self):
        while True:
            if not self.ingest['status']:
                self.usage_data['ingest'] = 0
                self.usage_data['available'] += self.ingest['demand']
                self.ingest['demand'] = 0
            if len(self.tasks['waiting']) > 0:
                for task in self.tasks['waiting']:
                    if task.task_status is TaskStatus.FINISHED:
                        self.tasks['waiting'].remove(task)
                        self.tasks['finished'].append(task)
                    if task.est >= self.env.now:
                        machine = self.dmachine[task.machine.id]
                        self.tasks['running'].append(task)
                        machine.run(task)
            yield self.env.timeout(1)

    def check_ingest_capacity(self, pipeline_demand, max_ingest_resources):
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

        Returns
        -------
            True if the cluster has capacity
            False if the cluster does not have capacity to run the pipeline
        """

        # Pipeline demand is the number of machines required for the pipeline
        # Length is how long the pipeline will take to
        # ingest/observation will take

        if len(self.resources['available']) >= pipeline_demand \
                and len(self.resources['ingest']) < max_ingest_resources:
            return True
        else:
            return False

    def provision_ingest_resources(self, demand, duration):
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

        tasks = self._generate_ingest_tasks(demand, duration)

        pairs = []
        self.resources['ingest'].extend(self.resources['available'][:demand])
        self.resources['available'] = self.resources['available'][demand:]

        self.usage_data['available'] = len(
            self.resources['available']
        )
        self.usage_data['ingest'] = len(self.resources['ingest'])

        for i, machine in enumerate(self.resources['ingest']):
            pairs.append((machine, tasks[i]))

        self.ingest['status'] = True
        self.ingest['demand'] = demand
        curr_tasks = {}
        while True:
            for pair in pairs:
                (machine, task) = pair
                if task not in self.tasks['running']:
                    self.tasks['running'].append(task)
                    ret = self.env.process(machine.run(task, self.env))
                    curr_tasks[task] = ret
                if curr_tasks[task].triggered:
                    task.task_status = TaskStatus.FINISHED
                    self.tasks['running'].remove(task)
                    self.tasks['finished'].append(task)
                    self.resources['available'].append(machine)
                    self.resources['ingest'].remove(machine)
                    # break
            if len(self.resources['ingest']) == 0:
                self.ingest['completed'] += 1
                self.ingest['status'] = False
                # We've finished ingest
                break
            else:
                yield self.env.timeout(duration - 1)

        return True

    # return True

    # Mark resources as 'in-use' for the given pipeline.
    # Runs the task on the machie
    # Create a 'dummy' task for each machine, of the duration of the
    # Observation

    # task.length = length
    # self.cluster.allocate_task(task, machine)
    # if task.task_status is TaskStatus.SCHEDULED:
    # 	self.cluster.running.append(task)

    def _generate_ingest_tasks(self, demand, duration):
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
            t = Task(i, env=self.env)
            t.duration = duration
            t.task_status = TaskStatus.SCHEDULED
            tasks.append(t)
        return tasks

    def availability(self):
        """ Returns
        -------
        availability: what resources are available
        """
        availability = None
        for machine in self.machines:
            if machine.current_task:
                availability += 1

        return availability

    def resource_use(self):
        """Returns the utilisation of the Cluster"""
        ustilisation = None
        for machine in self.machines:
            if machine.current_task:
                ustilisation += 1

        return ustilisation

    def stop_task(self, task):
        if self.tasks['running'].remove(task) and \
                self.tasks['finished'].append(task):
            return True
        else:
            raise Exception

    # TODO Place holder method
    def efficiency(self):
        """

        Returns
        -------
        efficiency: The efficiency of the cluster
        """
        efficiency = None
        for machine in self.machines:
            if machine.current_task:
                efficiency += 1
        return efficiency

    def to_df(self):
        df = pd.DataFrame()
        df['available_resources'] = [self.usage_data['available']]
        df['occupied_resources'] = [self.usage_data['occupied']]
        df['ingest_resources'] = [self.usage_data['ingest']]

        df['running_tasks'] = [len(self.tasks['running'])]
        df['finished_tasks'] = [len(self.tasks['finished'])]
        df['waiting_tasks'] = [len(self.tasks['waiting'])]

        return df

    def print_state(self):
        self.resources
        self.tasks
        self.ingest

        return {
            'machines': [machine.print_state() for machine in self.machines],
            'resources': repr(self.resources),
            'tasks': repr(self.tasks),
            'ingest': repr(self.ingest)
        }
