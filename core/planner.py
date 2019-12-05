import sys
from os import path

# sys.path.append(path.abspath('../../shadow'))

from shadow.classes.workflow import Workflow
from shadow.classes.environment import Environment
from shadow.algorithms.heuristic import heft as shadow_heft
import config_data

# BUFFER_OFFSET = config_data.buffer_offset

# from core.telescope import Observation
class Planner(object):
	def __init__(self, env, algorithm, envconfig):
		self.env = env
		self.envconfig = envconfig
		self.wfconfig = None
		self.algorithm = algorithm
		self.wfname = None

	def run(self, observation):
		# wfid = observation.name
		self.wfname = observation.name
		self.wfconfig = observation.workflow
		observation.plan = self.plan(self.algorithm)
		yield self.env.timeout(0)

	def plan(self, algorithm):

		wf = Workflow(self.wfconfig)
		wfenv = Environment(self.envconfig)
		wf.add_environment(wfenv)
		plan = Plan(self.wfname, None, None, None)
		if algorithm is 'heft':
			makespan = shadow_heft(wf)
			plan.makespan = makespan
		else:
			sys.exit("Other algorithms are not supported")

		plan.exec_order = wf.execution_order
		plan.allocation = wf.machine_alloc
		return plan


class Plan(object):
	"""
	Plan object contains a workflow id, which is the workflow to which it is associated
	A workflow is created from shadow library; however, the planner is the
	"""
	def __init__(self, workflow, exec_order, allocation, makespan):
		self.id = workflow
		self.exec_order = exec_order
		self.allocation = allocation
		self.makespan = makespan
		self.start_time = None

	# def apply_offset(self):
	# 	for self.allocation
	#



