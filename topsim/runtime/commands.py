# Copyright (C) 2025 RW Bunney

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
Command-line interface for the TopSim project
"""

import click
import sys

import enum

from importlib.metadata import version as vs

from topsim.runtime.parser import parse_experiment, DataUse

from topsim.utils.experiment import Experiment


# See topsim.utils.experiment.Experiment kwargs
PARAMS_DATA_COMBINATIONS = "data_combinations"
PARAMS_ALLOCATION_COMBINATIONS = "alloc_combinations"
PARAMS_SIMULATION_CONFIG = "configuration"
PARAMS_OUTPUT_DIR = "output"
PARMS_SCHEDULER_ARGUMENTS = "sched_args"

@click.group()
def cli():
    """
    Command-line interface for the TOpSim simulation environment.

    This tool is used to provide an interface with Experiments so that you do not have to
    write scripts to do so using the topsim.core library.
    """


@cli.command()
# @click.option("--module", default='user.telescope')
def version(module=''):
    """
    Print the current version of TOpSim
    """
    click.echo(f"TOpSim: {vs('topsim')}")  # using the {module} module.")


pass_params = click.make_pass_decorator(dict, ensure=True)

@cli.group(chain=True)
@pass_params
@click.option("-i",
              "--input",
              "input_config",
              required=True,
              help="Input configuration file for experiment.")
@click.option(
    "-p",
    "--planning",
    "planning",
    multiple=True,
    default=["topsim.user.plan.batch_planning.BatchPlanning"],
    help="The planning algorithm used to generate the WorkflowPlan.",
)
@click.option(
    "-s",
    "--scheduling",
    "scheduling",
    multiple=True,
    default=["topsim.user.schedule.batch_allocation.BatchProcessing"],
    help="The scheduling algorithm used to generate the WorkflowPlan.",
)
@click.option(
    "-d",
    "--data_usage",
    "data",
    type=click.Choice([e.value for e in DataUse], case_sensitive=False),
    help="The data usage option for calculating runtime duration",
    # default=[str(DataUse.EdgeData)],
    multiple=True,
    required=True,
)
@click.option(
    "-o",
    "--output_dir",
    "output_dir",
    type=click.Path(),
    help="The output directory for where the results are stored",
    required=True
)
def experiment(params, input_config, planning, scheduling, data: DataUse,
               output_dir):

    pac, pdc = parse_experiment(planning, scheduling, data)

    params[PARAMS_ALLOCATION_COMBINATIONS] = pac
    params[PARAMS_DATA_COMBINATIONS] = pdc
    params[PARAMS_SIMULATION_CONFIG] = input_config
    params[PARAMS_OUTPUT_DIR] = output_dir


@experiment.command()
@pass_params
@click.option(
    "--ignore_ingest",
    "ignore_ingest",
    default=False,
)
@click.option(
    "--use_workflow_dop",
    "use_workflow_dop",
    default=True
)
def scheduler_options(params, **kwargs):
    """
    Add runtime options for scheduling heuristics
    """
    params[PARMS_SCHEDULER_ARGUMENTS] = kwargs

@experiment.command()
@pass_params
def describe(params):
    e = Experiment(**params)
    e.describe()

@experiment.command()
@pass_params
def run(params):
    """
    Run the experiment

    """
    click.echo(f"Running experiment with {params}")
    e = Experiment(
        **params
    )
    e.run()

if __name__ == '__main__':
    experiment()