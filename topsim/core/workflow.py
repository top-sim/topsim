class WorkflowPlan(object):
    """
    WorkflowPlans are used within the Planner, SchedulerA Actors and Cluster Resource. They are higher-level than the
    shadow library representation, as they are a storage component of scheduled tasks, rather than directly representing
    the DAG nature of the workflow. This is why the tasks are stored in queues.
    """

    def __init__(self, plan):
        self.plan = plan
        self.task_order = plan.exec_order
        self.allocation = plan.allocation
        self.priority = 0

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority

    def __gt__(self, other):
        return self.priority > other.priority


class Task(object):
    """
    Tasks have priorities inheritted from the workflows from which they are arrived; once
    they arrive on the cluster queue, they are workflow agnositc, and are processed according to
    their priority.
    """

    def __init__(self, id, env=None, pred=None):
        self.env = env
        self.id = id
        self.start = 0
        self.finish = 0
        self.flops = 0
        self.memory = None
        self.io = 0
        self.alloc = None
        self.duration = None
        self.pred = pred
