"""
Algorithm presents the abstract base class for any Scheduling algorithm.
"""


from abc import ABC, abstractmethod


class Algorithm(ABC):
    """
    Abstract base class for all Scheduling Algorithms (used in the dynamic
    allocation by the 'scheduler').
    """

    def __init__(self):
        self.name = "AbstractAlgorithm"

    @abstractmethod
    def run(self, cluster, clock, plan, schedule):
        """

        Parameters
        ----------
        cluster
        clock
        plan
        schedule

        Returns
        -------

        """

        pass

    @abstractmethod
    def to_df(self):
        """
        Produce a Pandas DataFrame object to return current state of the
        scheduling algorithm

        Returns
        -------
        df : pandas.DataFrame
            DataFrame with current state
        """
