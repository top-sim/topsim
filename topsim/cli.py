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

from importlib.metadata import version as vs


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
    click.echo(f"TOpSim: {vs('topsim')}") # using the {module} module.")


if __name__ == '__main__':
    cli()