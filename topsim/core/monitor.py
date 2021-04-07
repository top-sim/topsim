import json
import logging
import time
import os
import pandas as pd
logger = logging.getLogger(__name__)


class Monitor(object):
    def __init__(self, simulation,start_time):
        self.simulation = simulation
        self.env = simulation.env
        self.sim_timestamp = start_time
        self.events = []
        self.df = pd.DataFrame()

    def run(self):
        while True:
            logger.debug('SimTime=%s', self.env.now)
            # time.sleep(0.5)
            state = {
                'cluster_state': self.simulation.cluster.print_state(),
                'instrument_state': self.simulation.instrument.print_state(),
                'scheduler_state': self.simulation.scheduler.print_state(),
            }

            self.df = self.df.append(
                self.collate_actor_dataframes(), ignore_index=True
            )
            tasks = self.simulation.cluster.finished_task_time_data()
            palg = self.simulation.planner.algorithm
            salg = repr(self.simulation.scheduler.algorithm)
            tasks.to_pickle(f"{self.sim_timestamp}-{palg}-{salg}-tasks.pkl")
            self.df.to_pickle(f"{self.sim_timestamp}-{palg}-{salg}-sim.pkl")
            yield self.env.timeout(1)

    def write_to_file(self):
        with open(self.sim_timestamp, 'w+') as f:
            json.dump(self.events, f, indent=4)

    def collate_actor_dataframes(self):
        df = pd.DataFrame()
        cluster = self.simulation.cluster.to_df()
        buffer = self.simulation.buffer.to_df()
        instrument = self.simulation.instrument.to_df()
        df = df.join([cluster, buffer, instrument], how='outer')
        return df

