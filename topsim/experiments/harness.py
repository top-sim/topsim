# Copyright (C) 2/9/20 RW Bunney

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Running experiments in TOpSim requires more than just a simulation object;
there needs to be a sequence of tests that are run, and in such a way that
the results are reproducible and bundled effectively.
"""
import threading
import numpy as np


class ExperimentRunner:
	"""
	ExperimentRunner is a class that helps run multiple instances of TOpSim
	simulations to acheive a comprehensive, broader set of results on
	simulations than would be achieve by running a single simulation in
	series.

	An experiment has a set of known outputs that we want to use for testing
	certain hypotheses/relationships. Wrapping a simulation up into an
	experiment allows us to define what relationships we want to observe and
	then automatically produce results from the simulation. This takes
	longer, but the analysis is automated and reproducible, as it is built
	into the experiment.
	"""

	def __init__(self, threads, experiments=None):
		self.threads = threads
		self.sim_sequence = []
		for exp in experiments:
			simulations = exp.generate_simulations_from_parameters()
			self.sim_sequence.append(simulations)

	def run(self):
		splits = np.array_split(self.sim_sequence, self.threads)
		for sequence in splits:
			t = threading.Thread(target=self.run_sim_sequence, args=sequence)

	def run_sim_sequence(self, sequence):
		for sim in sequence:
			sim.run(-1)


class Experiment:
	"""
	An experiment is a single-hypothesis-driven object that encapsulates a
	set of inputs and expected outputs for a set of simulations. The purpose
	of an experiment object is to provide a a programmable delineation
	between variables that are controlled, dependent, and independent over a
	number of simulations; this helps for experiment organisation, as well as
	reproducibility of simulation results.

	This requires a reflection on what we can control, change, and measure
	during a single simulation - and then how we can accurately measure the
	relationships between these over a number of related simulations,
	which form a single Experiment.

	Parameters
	----------
	controls : dict
		controls is a dictionary of variables from the set of allowed
		variables, with key-values being

		'variable': 'file_path_to_config'

		For example,

		'buffer': 'buffer_config_file'

		Controls should list a single file path, as there should be no
		multiplicity of specifications in a controlled variable.

	independents : dict


	"""

	def __init__(self, controls=None, independents=None):
		if not controls or not independents:
			raise RuntimeError("Experiment has not got adequate information")
		self.controls = controls
		self.independents = independents
