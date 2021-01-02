import json
import logging
import time

import pandas as pd
logger = logging.getLogger(__name__)


class Monitor(object):
    def __init__(self, simulation):
        self.simulation = simulation
        self.env = simulation.env
        self.event_file = simulation.event_file
        self.events = []
        self.df = pd.DataFrame()

    def run(self):
        while True:
            logger.debug('SimTime=%s', self.env.now)
            # time.sleep(0.5)
            state = {
                # 'timestamp': self.env.now,
                'cluster_state': self.simulation.cluster.print_state(),
                'telescope_state': self.simulation.telescope.print_state(),
                'scheduler_state': self.simulation.scheduler.print_state(),
                'buffer_state': self.simulation.buffer.print_state()
            }

            self.df = self.df.append(
                self.collate_actor_dataframes(), ignore_index=True
            )

            logger.debug("Storing state %s",
                         self.simulation.scheduler.print_state())
            self.events.append(state)
            self.write_to_file()
            # self.df = self.df.append(cluster)
            # x = [buffer, self.df]
            # self.df = self.df.merge(buffer, how='left',sort=False)
            alg = self.simulation.planner.algorithm
            self.df.to_pickle("{0}-{1}.pkl".format(self.event_file,alg))
            yield self.env.timeout(1)

    def write_to_file(self):
        with open(self.event_file, 'w+') as f:
            json.dump(self.events, f, indent=4)

    def collate_actor_dataframes(self):
        df = pd.DataFrame()
        cluster = self.simulation.cluster.to_df()
        buffer = self.simulation.buffer.to_df()
        telescope = self.simulation.telescope.to_df()
        df = df.join([cluster, buffer, telescope], how='outer')
        return df

