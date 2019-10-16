import sys
from os import path

# sys.path.append(path.abspath('../../shadow'))

from shadow.classes.workflow import Workflow
from shadow.classes.environment import Environment
from shadow.algorithms.heuristic import heft as shadow_heft

class Planner(object):
	def __init__(self, env, algorithm, envconfig):
		self.env = env
		self.envconfig = envconfig
		self.wfconfig = None
		self.algorithm = algorithm

	def run(self, obseration):
		self.plan(self.algorithm)
		pass

	def plan(self, algorithm):
		wf = Workflow(self.workflow)
		wfenv = Environment(self.envconfig)
		wf.add_environment(wfenv)
		plan = Plan(self.wfconfig, )
		if algorithm is 'heft':
			shadow_heft(wf)

		else:
			sys.exit("Other algorithms are not supported")


class Plan(object):
	"""
	Plan object contains a workflow id, which is the workflow to which it is associated
	A workflow is created from shadow library; however, the planner is the
	"""
	def __init__(self, workflow):
		self.id = workflow