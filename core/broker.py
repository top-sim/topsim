from core.job import Job


class Broker(object):
    def __init__(self, env, job_configs):
        self.env = env
        self.simulation = None
        self.cluster = None
        self.destroyed = False
        self.job_configs = job_configs
        self.waiting_workflows = []

    def attach(self, simulation):
        self.simulation = simulation
        self.cluster = simulation.cluster

    def run(self):
        print("Broker is waiting")
        if self.waiting_workflows:
            print("Workflows currently waiting with the Broker: {0}".format(self.waiting_workflows))
        for workflow in self.waiting_workflows:
            assert workflow >= self.env.now
            yield self.env.timeout(workflow.submit_time - self.env.now)
            # print('a task arrived at time %f' % self.env.now)
            self.cluster.add_workflow(workflow)
        # self.destroyed = True

    def add_workflow(self, workflow):
        print("Adding", workflow, "to workflows")
        # Instatiate a new workflow object with the submission time
        self.waiting_workflows.append(workflow)
        print("Waiting workflows", self.waiting_workflows)

    """
    The broker is going to accumulate workflows over time. At each time point, it 
    is going to check if it has any new workflows; if it does, it will go and fetch
    the information about that workflow (intended start time of first task etc.). This 
    expected start time is produced by the planner, which should take into account 
    the expected exit time of the observation, and a 'buffer delay' that is specified at 
    the start of the simulation.  
    """