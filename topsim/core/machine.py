import pandas as pd

from enum import Enum

# TODO need I/O information here
from topsim.core.task import TaskStatus


class Status(Enum):
    IDLE = 0
    IN_USE = 1
    RESERVED = 2
    ERROR = 3


class Machine(object):
    def __init__(self, id, cpu, memory, disk, bandwidth):
        self.id = id
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.bandwidth = bandwidth
        self.status = Status.IDLE
        self.transfer_flag = False
        self.current_task = None

    def run(self, task, env, predecessor_allocations):
        # return True
        while True:
            if task.task_status is TaskStatus.SCHEDULED:
                self.run_task(task)
                ret = None
                ret = env.process(task.do_work(env,self,predecessor_allocations))
                self.stop_task(task)
                return ret

    def run_task(self, task_instance):
        self.cpu -= task_instance.flops
        self.memory -= task_instance.task_data
        self.disk -= task_instance.io
        self.current_task = task_instance
        # self.task_instances.append(task_instance)
        self.status = Status.IN_USE

    def stop_task(self, task_instance):
        self.cpu += task_instance.flops
        self.memory += task_instance.task_data
        self.disk += task_instance.io
        self.status = Status.IDLE
        self.current_task = None

    def to_df(self):
        d = {
            'id': [self.id],
            'cpu': [self.cpu],
            'memory': [self.memory],
            'disk': [self.disk]
        }
        df = pd.DataFrame.from_dict(d)
        return df

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, Machine) and other.id == self.id

    def __repr__(self):
        return str(self.id)

    def print_state(self):
        return {
            'id': self.id,
            'cpu': self.cpu,
            'memory': self.memory,
            'disk': self.disk,
        }


def utilisation_policy(machine):
    return None
