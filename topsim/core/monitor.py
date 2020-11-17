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
            cluster = self.simulation.cluster.to_df()
            buffer = self.simulation.buffer.to_df()
            machines = state['cluster_state']

            logger.debug("Storing state %s",
                         self.simulation.scheduler.print_state())
            self.events.append(state)
            self.write_to_file()
            self.df = self.df.append(cluster,ignore_index=True)
            x = [buffer, self.df]
            self.df = self.df.join(buffer, how='outer')
            self.df.to_pickle("{}.pkl".format(self.event_file))
            yield self.env.timeout(1)

    def write_to_file(self):
        with open(self.event_file, 'w+') as f:
            json.dump(self.events, f, indent=4)
