import json
import logging
import time
import os
import pandas as pd

logger = logging.getLogger(__name__)


class Monitor(object):
    """
    The Monitor actor for a TopSim simulation

    Parameters
    ----------
    simulation : topsim.core.Simulation
        The simulation object

    Attributes
    ----------
    """
    def __init__(self, simulation, start_time):
        self.simulation = simulation
        self.env = simulation.env
        self.sim_timestamp = start_time
        self.df = pd.DataFrame()
        self.events = pd.DataFrame()

    def run(self):
        while True:
            if self.env.now % 1000 == 0:
                logger.debug('SimTime=%s', self.env.now)
            # time.sleep(0.5)
            self.df = pd.concat(
                [self.df, self.collate_actor_dataframes()],
                ignore_index=True
            )
            self.collate_events()
            yield self.env.timeout(1)

    import h5py

    def collate_actor_dataframes(self):
        """
        Take information on a per-timestep basis from each Actor and collate
        it for appending to total simulation data.

        Each actor returns a dataframe with timestep data. This data is
        appeneded to a dataframe represents one timestep for the entir
        Returns
        -------

        """
        df = pd.DataFrame()
        cluster = self.simulation.cluster.to_df()
        buffer = self.simulation.buffer.to_df()
        instrument = self.simulation.instrument.to_df()
        scheduler = self.simulation.scheduler.to_df()
        delay = pd.DataFrame(
            {'delay': [self.simulation.planner.delay_model.degree.value]}
        )
        algs = pd.DataFrame(
            {
                'planning': [str(self.simulation.planner.model.algorithm)],
                'scheduling': [str(self.simulation.scheduler.algorithm)]
            },
        )
        cf = pd.DataFrame({'config': [str(self.simulation._cfg_path.name)]})
        df = df.join(
            [cluster, buffer, instrument, scheduler, algs, cf, delay],
            how='outer'
        )
        return df

    def collate_events(self):
        """
        Update events attribute with this timestep's events

        Returns
        -------

        """
        if self.simulation.instrument.events:
            self.events = pd.concat([self.events,
                                    pd.DataFrame(self.simulation.instrument.events)])

        if self.simulation.scheduler.events:
            self.events = pd.concat([self.events,
                                    pd.DataFrame(self.simulation.scheduler.events)])
        if self.simulation.buffer.events:
            self.events = pd.concat([self.events,
                                    pd.DataFrame(self.simulation.buffer.events)])

        self.events = self.events.infer_objects()
