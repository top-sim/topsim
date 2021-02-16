# Copyright (C) 12/10/20 RW Bunney

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

from enum import Enum
import logging

from numpy.random import default_rng, seed

LOGGER = logging.getLogger(__name__)


class DelayModel:
    """
    The delay model is the delay or failure model for tasks in a workflow.
    If we have a possibility for delay, then the timeout triggered by a process
    will be increased by a certain portion.

    A delay model object may be passed to any of the actors within the
    simulation that yield a timeout to the environment. Each actor will store
    their expected (current) timeouts and, if present, the actual timeout
    if a delay has been triggered.

    Attributes
    ----------
    prob : float
        The probabilty a delay will occur

    dist : str
        String name of the distribution from which values will be picked to
        create a delay.

        Currently normal, poisson, and uniform distributions from numpy are
        supported. If

    degree : enumerate.Enum (float)
        The 'degree' to which we will be generating the delay. The higher
        degree, the larger the final delay.
    """

    class DelayDegree(Enum):
        LOW = 0.25
        MID = 0.5
        HIGH = 0.75

    def __init__(self, prob, dist, degree=DelayDegree.LOW, seed=20):
        """

        Parameters
        ----------
        prob : float
            probability that a delay will occur

        dist : str
            String name of the distribution from which values will be picked to
            create a delay.

        degree : enumerate.Enum (float)
            The 'degree' to which we will be generating the delay. The higher
            degree, the larger the final delay.

        seed : int
            The input seed to ensure repeatable randomness
        """

        _allowed_dist = ['normal', 'poisson', 'uniform']
        if dist not in _allowed_dist:
            LOGGER.debug(
                "Distribution function not specified; defaulting "
                "to uniform distribution."
            )
            dist = 'uniform'
        self.prob = prob
        self.dist = dist
        self.degree = degree
        self.seed = seed

    def generate_delay(self, task_runtime, n=100):
        """
        Produce a delay based on current DelayModel attributes.
        A delay is a unit of time to be passed to the timeout.

        Given a probability, return the new delay by a factor of 'degree'
        Returns
        -------
        delay : int
            This is the runtime+delay value = essentially the new runtime value
        """
        delay = task_runtime
        if default_rng(self.seed).random() < self.prob:
            rand_var = self._create_random_value_from_runtime(task_runtime, n)
            delay = int(rand_var)
        return delay

    def _create_random_value_from_runtime(self, runtime, n=100):
        """
        Take a runtime value and generate a distribution from that value
        using the self.dist distrubution, based on the numpy distribution
        function.

        This distribution uses the runtime value as the mean (mu) value

        Returns
        -------
        rand_var : int
            Random integer variable drawn from a distribution based on the
            runtime value
        """
        s = None
        mu = runtime
        sigma = self.degree.value * mu

        if self.dist is "normal":
            s = default_rng(self.seed).normal(mu, sigma, n)
        elif self.dist is "poisson":
            s = default_rng().poisson(mu, int(runtime / self.degree))
        else:
            s = default_rng().uniform()

        var = s[s > mu]
        rand_var = var[int(len(var)/2)]
        return rand_var

