import core.simulation
import sys
import argparse
from core.simulation import Simulation
from core import config

import test_data
import test.test_buffer, test.test_cluster, test.test_planner, test.test_scheduler
from core.cluster import Cluster
import simpy
import unittest

from core.machine import Machine
from core.planner import Planner
from core.scheduler import Scheduler
# from core.buffer import Broker
from core.simulation import Simulation
from core.telescope import Telescope, Observation
from core.buffer import Buffer
# from algorithms.random_algorithm import RandomAlgorithm
from algorithms.scheduling import FifoAlgorithm

"""
topsim.py takes command-line arguments to specify how the simulation will run
It can also be used to run tests 
"""
testcases = {  # Tests for the test runner
	"buffer": test.test_buffer,
	"cluster": test.test_cluster,
	"planner": test.test_planner,
	"algorithms": test.test_scheduler
}


def run_tests(arg, tests, test_parser):
	if arg['all']:
		suite = unittest.TestSuite()
		loader = unittest.TestLoader()
		for test in tests:
			suite.addTests(loader.loadTestsFromModule(tests[test]))
		runner = unittest.TextTestRunner()
		runner.run(suite)
		return True
	# Tests do not exist yet. This is something to add in the future, when we have end-to-end functionality
	pass


def run_algorithm():
	pass


def run_simulation(arg, parser):
	workflow_file = None
	print(arg['workflow'])
	if arg['workflow']:
		workflow_file = arg['workflow']
	else:
		parser.print_help()
	event_file = 'sim.trace'
	planning_algorithm = 'heft'
	env = simpy.Environment()
	tmax = 36  # for starters, we will define telescope configuration as simply number of arrays that exist
	salgorithm = FifoAlgorithm()
	vis = False
	if arg['visualise']:
		vis = True

	simulation = Simulation(env,
							test_data.telescope_config,
							tmax,
							test_data.machine_config,
							salgorithm,
							'heft',
							event_file,
							vis)

	simulation.start(-1)


# env.run(until=100)  # until=100)


def run_scheduler():
	pass


def run_planner():
	pass


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='TopSim: Telescope Operations Simulator')

	subparsers = parser.add_subparsers(dest='command')

	# choices = list(cfg.test.keys())
	# test = { # Tests for the test runner
	# 	"workflow": test.test_workflow,
	# 	"graph_generator": test.test_graph_generator,
	# 	"heuristic": test.test_heuristic,
	# 	"metaheuristic": test.test_metaheuristic}

	test_parser = subparsers.add_parser('test', help='Test Runner')
	test_parser.add_argument('--all', action='store_true', help='Run all test')

	test_parser.set_defaults(func=run_tests)

	simulation_parser = subparsers.add_parser('sim', help='Run a simulation')
	simulation_parser.set_defaults(func=run_simulation)
	simulation_parser.add_argument('workflow', help='Name of algorithm')
	simulation_parser.add_argument('--calc_time', help='Set calc_time True/False', choices=['True', 'False'])
	simulation_parser.add_argument('--visualise', help='Visualise the simulation', choices=['True', 'False'])

	args = parser.parse_args()
	if not args.command:
		parser.print_help()
	if args.command == 'sim':
		args.func(vars(args), simulation_parser)
	if args.command == 'test':
		args.func(vars(args), testcases, test_parser)


