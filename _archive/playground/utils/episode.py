import simpy
from topsim.core.cluster import Cluster
from topsim.core import Scheduler
from topsim.core.buffer import Broker
from topsim.core.simulation import Simulation


class Episode(object):
    def __init__(self, machine_configs, task_configs, algorithm, event_file):
        self.env = simpy.Environment()
        cluster = Cluster()
        cluster.add_machines(machine_configs)

        task_broker = Broker(self.env, task_configs)

        scheduler = Scheduler(self.env, algorithm)

        self.simulation = Simulation(self.env, cluster, task_broker, scheduler, event_file)

    def run(self):
        self.simulation.start()
        self.env.run()
