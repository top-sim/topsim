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
import gc
import os
import sys
import time

import logging
from builtins import enumerate

import simpy
from datetime import date

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)

# Framework defined models
from topsim.core.simulation import Simulation
from topsim.core.delay import DelayModel

# User defined models
from topsim.user.telescope import Telescope  # Instrument
from topsim.user.schedule.batch_allocation import BatchProcessing
from topsim.user.plan.batch_planning import BatchPlanning  # Planning
from topsim.user.plan.static_planning import SHADOWPlanning
from topsim.user.schedule.dynamic_plan import DynamicSchedulingFromPlan


class Experiment:
    """
    Experiment wraps the constructions of initialisation a series of one or more related simulations together,
    to avoid significantly large script files.


    """

    def __init__(self, configuration: list() = None, alloc_combinations: list(tuple()) = None,
                 output=None, delay: bool = False):
        self._configuration = configuration
        self._combinations = alloc_combinations
        self._delay = delay
        self._output = output
        self._sims = []

    def _build_simulations(self):
        for c in self._configuration:
           for ac in self._combinations:
                plan, sched = ac
                if plan == "batch":
                    plan = BatchPlanning("batch")
                elif plan == "static":
                    plan = SHADOWPlanning("heft")
                else:
                    raise RuntimeError("Planning '%s' is not supported", plan)

                if sched == "dynamic_plan":
                    sched = DynamicSchedulingFromPlan()
                else:
                    sched = BatchProcessing(min_resources_per_workflow=1, resource_split={}, max_resource_partitions=1)
                env = simpy.Environment()
                instrument = Telescope
                yield Simulation(env=env, config=c, instrument=instrument,
                                        planning_model=plan, scheduling=sched, delay=self._delay, timestamp=None,
                                        to_file=True,
                                        hdf5_path=f"{self._output}/results_f{date.today().isoformat()}.h5")



    def _review_experiment_combinations(self):
        pass
    import gc

    # from memory_profiler import profile
    #
    # @profile
    def run(self, review=False, threading=False):
        if not self._output:
            LOGGER.warning("No output file set, experiments will not be run.")
            return ()
        i = 0
        for s in self._build_simulations():
        # self._review_experiment_combinations()
        # i = 0
        #     s = self._sims[i]
        # for i, s in enumerate(self._sims):
            LOGGER.info("Simulation %s/%s running...", i+1, len(self._combinations) * len(self._configuration))
            LOGGER.info("Simulation is using %s to plan and %s to schedule",
                        s.planner.model.algorithm, s.scheduler.algorithm)
            print(s.planner.model.algorithm, s.scheduler.algorithm)
            st = time.time()
            try:
                s.start()
            except ValueError:
                print(f"Simulation {i+1} did not run due to non-useable simulation parameters")
            # self._sims.remove(s)
            gc.collect()
            ft = time.time()
            i += 1
            LOGGER.info("Runtime: %s.", ft - st)
            if i >=5:
                break
        LOGGER.info("Experiment complete.")


class ExperimentData:

    def __init__(self):
        pass