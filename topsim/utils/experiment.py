# Copyright (C) 2024 RW Bunney

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
Wrapper class for running multiple iterations of simulations using
standard simulation setup.

This provides an example implementation and sits alongside the
user.* modules implemented in this codebase.
"""

import time
import itertools
import logging

import simpy
from datetime import date
from pathlib import Path

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)

# Framework defined models
from topsim.core.simulation import Simulation

# User defined models
from topsim.user.telescope import Telescope  # Instrument
from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.plan.batch_planning import BatchPlanning  # Planning
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.schedule.dynamic_plan import DynamicSchedulingFromPlan


class Experiment:
    """
    Experiment wraps the constructions of initialisation a series of one or more related simulations together,
    to avoid significantly larger script files.

    Experiment has two different modes:
    - Serial
    - Batch

    """

    def __init__(
            self,
            configuration: list = None,
            alloc_combinations: list[tuple] = None,
            data_combinations: list[tuple] = None,
            output=None,
            delay: bool = False,
            **kwargs):

        self._configurations = configuration
        self._combinations = list(itertools.product(alloc_combinations,
                                                    data_combinations))
        self._delay = delay
        self._output = Path(output)
        self._sims = []
        self.sched_args = kwargs['sched_args']
        self._batch = kwargs['slurm'] if 'batch' in kwargs else False

    def _build_simulations(self):
        if not self._output.exists():
            try:
                self._output.mkdir(parents=True)
            except OSError as e:
                LOGGER.critical("Failed to make output directory: %s", e)
        for c in self._configurations:
           for combination in self._combinations:
                ac, dc = combination
                plan, sched = ac
                use_task_data, use_edge_data = dc
                if plan == "batch":
                    plan = BatchPlanning("batch")
                elif plan == "static":
                    plan = SHADOWPlanning("heft")
                else:
                    raise RuntimeError("Planning '%s' is not supported", plan)

                if sched == "dynamic_plan":
                    sched = DynamicSchedulingFromPlan(**self.sched_args)
                else:
                    sched = BatchProcessing(**self.sched_args)
                env = simpy.Environment()
                instrument = Telescope
                result_path_hash = _generate_truncated_hash(c, hash_length=6)
                yield Simulation(env=env, config=c, instrument=instrument,
                                 planning_model=plan, scheduling=sched, delay=self._delay, timestamp=None,
                                 to_file=True,
                                 hdf5_path=f"{self._output}/results_f{date.today().isoformat()}_{result_path_hash}.h5",
                                 use_task_data=use_task_data, use_edge_data=use_edge_data)

    def _run_batch(self):
        """
        Batch experiments are single-run experiments, which means we don't run combinations
        """ 
        c= self._configurations[0]
        plan, sched = self._combinations[0]
        if plan == "batch":
            plan = BatchPlanning("batch")
        elif plan == "static":
            plan = SHADOWPlanning("heft")
        else:
            raise RuntimeError("Planning '%s' is not supported", plan)

        if sched == "dynamic_plan":
            sched = DynamicSchedulingFromPlan(**self.sched_args)
        else:
            sched = BatchProcessing(**self.sched_args)
        env = simpy.Environment()
        instrument = Telescope
        result_path_hash = _generate_truncated_hash(c, hash_length=6)
        yield Simulation(env=env, config=c, instrument=instrument,
                            planning_model=plan, scheduling=sched, delay=self._delay, timestamp=None,
                            to_file=True,
                            hdf5_path=f"{self._output}/results_f{date.today().isoformat()}_{result_path_hash}.h5")



    def _review_experiment_combinations(self):
        pass

    def run(self, review=False, threading=False):
        """
        Run a combinations of simulations based on parameters provided to the class
        constructor.

        Parameters
        ----------
        review
        threading

        Returns
        -------

        """
        if not self._output:
            LOGGER.warning("No output file set, experiments will not be run.")
            return exit(1)
        if self._batch:
            s = self._run_batch()
            st = time.time()
            LOGGER.info("Simulation is using %s to plan and %s to schedule",
                            s.planner.model.algorithm, s.scheduler.algorithm)
            try:
                    s.start()
            except ValueError as exp:
                LOGGER.warning("Simulation  did not run due to non-useable simulation "
                        "parameters")
                ft = time.time()
            LOGGER.info("Runtime: %s.", ft - st)
        else:
            i = 0
            for s in self._build_simulations():
                LOGGER.info("Simulation %s/%s running...",
                            i+1, len(self._combinations) * len(self._configurations))
                LOGGER.info("Simulation is using %s to plan and %s to schedule",
                            s.planner.model.algorithm, s.scheduler.algorithm)
                print(s.planner.model.algorithm, s.scheduler.algorithm)
                st = time.time()
                try:
                    s.start()
                except ValueError as exp:
                    print(exp)
                    print(f"Simulation {i+1} did not run due to non-useable simulation "
                        f"parameters")
                # self._sims.remove(s)
                ft = time.time()
                i += 1
                LOGGER.info("Runtime: %s.", ft - st)
        LOGGER.info("Experiment complete.")

def _generate_truncated_hash(path: Path, hash_length: int ) -> str:
    """
    Generate a truncated string hash of the pathname. This is to balance 
    result file pathname uniqueness and readability. 
    
    This is _not_ a secure hash and will definitely increase the chance of 
    conflicts. However, we are not expecting to produce large numbers of result
    files over the course a given experiment so it is unlikely to produce 
    collisions (e.g. ~150 different output files). 

    Parameters: 
        path: name of the path we are hashing
        hash_length: The length of the truncated hash

    Returns:
        Truncated result of the absolute value of hash(path). 
    """

    return str(abs(hash(path)))[:hash_length]