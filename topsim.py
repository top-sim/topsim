import argparse

from test import test_scheduler, test_buffer,test_cluster,test_planner
import simpy
import simpy.rt
import unittest

import config.data as test_data
# from core.buffer import Broker
from core.simulation import Simulation
# from algorithms.random_algorithm import RandomAlgorithm
from algorithms.scheduling import FifoAlgorithm

"""
topsim.py takes command-line arguments to specify how the simulation will run
It can also be used to run tests 
"""
testcases = {  # Tests for the test runner
	"buffer": test_buffer,
	"cluster": test_cluster,
	"planner": test_planner,
	"algorithms": test_scheduler
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


if __name__ == '__main__':

	workflow_file = 'test/data/daligue_pipeline_test.json'
	event_file = 'sim.trace'
	planning_algorithm = 'heft'
	# env = simpy.rt.RealtimeEnvironment(factor=0.5, strict=False)# Environment()
	tmax = 36  # for starters, we will define telescope configuration as simply number of arrays that exist
	env = simpy.Environment()
	scheduling_algorithm = FifoAlgorithm()
	vis = False

	# TODO move things like 'heft' into a 'common' file which has SchedulingAlgorithm.HEFT = 'heft' etc.
	simulation = Simulation(
		env,
		test_data.telescope_config,
		tmax,
		test_data.machine_config,
		scheduling_algorithm,
		planning_algorithm,
		event_file,
		vis)
	simulation.start(-1)



