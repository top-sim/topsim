import core.simulation 
import sys
import argparse
from core.simulation import Simulation
from core import config

import config_data

from core.cluster import Cluster
import simpy
from core.machine import Machine
from core.planner import Planner
from core.scheduler import Scheduler
from core.broker import Broker
from core.simulation import Simulation
from core.telescope import Telescope, Observation, Buffer
from scheduler.random_algorithm import RandomAlgorithm

"""
topsim.py takes command-line arguments to specify how the simulation will run
It can also be used to run tests 
"""


def run_tests():
	# Tests do not exist yet. This is something to add in the future, when we have end-to-end functionality
	pass


def run_algorithm():
	pass


def run_simulation(arg,parser):
	workflow_file = None
	print(arg['workflow'])
	if arg['workflow']:
		workflow_file = arg['workflow']
	else:
		parser.print_help()
	event_file = '/home/rwb/github/topsim/sim.trace'
	algorithm = RandomAlgorithm()
	# Copied from playground.episode
	machines_number = 5
	jobs_len = 10
	jobs_csv = './workflows.csv'
	machines = config.process_machine_config(config_data.machine_config)
	observations = config.process_telescope_config(config_data.telescope_config)
	task_configs = []
	env = simpy.Environment()
	cluster = Cluster()
	cluster.add_machines(machines)
	task_broker = Broker(env, task_configs)
	scheduler = Scheduler(env, algorithm)
	planner = Planner(env, 'heft', config_data.machine_config)

	tconfig = 36  # for starters, we will define telescope configuration as simply number of arrays that exist
	# [start_time, duration, num_arrayssimple_sim.py:40_used]
#	observation_data = [emu, dingo, vast]  # , [40, 10, 36]]
	buffer = Buffer(env)

	telescope = Telescope(env, observations, buffer, tconfig,planner)

	simulation = Simulation(env, telescope, cluster, task_broker, scheduler, event_file)

	simulation.init_process()
	env.run(until=100)  # until=100)


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

	args = parser.parse_args()
	if not args.command:
		parser.print_help()
	if args.command == 'sim':
		args.func(vars(args), simulation_parser)
	if args.command == 'test':
		args.func(vars(args), test_parser)
