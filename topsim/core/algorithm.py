"""
Algorithm presents the abstract base class for any Scheduling algorithm.
"""


from abc import ABC, abstractmethod


class Algorithm(ABC):
    """
    Abstract base class for all Scheduling Algorithms (used in the dynamic
    allocation by the 'scheduler').
    """
    @abstractmethod
    def __call__(self, cluster, clock, plan):
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
