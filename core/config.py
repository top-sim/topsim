import json
from core.telescope import Telescope, Observation
from core.machine import Machine
import pandas as pd


import logging

logger = logging.getLogger(__name__)


# class Workflow(object):
# 	def __init__(self,workflow):
# 		self.id = workflow
# 		self.submit_time = 0
#


# Helper function that acts as static function for Cluster
def process_machine_config(machine_config):
	config = None
	try:
		with open(machine_config, 'r') as infile:
			config = json.load(infile)
	except OSError:
		logger.warning("File %s not found", machine_config)
		raise
	except json.JSONDecodeError:
		raise
	try:
		'system' in config and 'resources' in config['system']
	except KeyError:
		logger.warning(
			"'system' is not in %s, check your JSON is correctly formatted",
			machine_config
		)
		raise
	machines = config['system']['resources']
	machine_list = []
	for machine in machines:
		machine_list.append(
			Machine(
				id=machine,
				cpu=machines[machine]['flops'],
				memory=1,
				disk=1,
				bandwidth=machines[machine]['rates']
			)
		)
	return machine_list


def process_telescope_config(telescope_config):
	observations = []
	infile = open(telescope_config)
	config = pd.read_csv(infile)
	# config.
	# Format is name, start, duration, demand, filename
	for i in range(len(config)):
		obs = config.iloc[i, :]
		observation = Observation(
			obs['name'],
			int(obs['start']),
			int(obs['duration']),
			int(obs['demand']),
			obs['filename']
		)
		observations.append(observation)
	infile.close()
	return observations


#
#
# def process_telescope_config(telescope_config):
# 	return Telescope


class TaskInstanceConfig(object):
	def __init__(self, task_config):
		self.cpu = task_config.cpu
		self.memory = task_config.memory
		self.disk = task_config.disk
		self.duration = task_config.duration


class TaskConfig(object):
	def __init__(self, task_index, instances_number, cpu, memory, disk,
				 duration):
		self.task_index = task_index
		self.instances_number = instances_number
		self.cpu = cpu
		self.memory = memory
		self.disk = disk
		self.duration = duration


class JobConfig(object):
	def __init__(self, idx, submit_time, task_configs):
		self.submit_time = submit_time
		self.task_configs = task_configs
		self.id = idx

	def __repr__(self):
		return str(self.id)
