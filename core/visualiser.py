# Copyright (C) 17/1/20 RW Bunney

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
import time

"""
Use 'bokeh' python library to visualise the current state of the system
"""
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.models import LinearColorMapper
from bokeh.models.widgets import Button, DataTable, TableColumn
from bokeh.server.server import Server
from threading import Thread
from bokeh.layouts import layout
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure

import logging
logger = logging.getLogger(__name__)


class Visualiser(object):
	def __init__(self, simulation):
		self.simulation = simulation
		self.env = self.simulation.env
		self.events = []
		self.not_started = True
		self.start = False
		logger.debug('Opening Bokeh application on http://localhost:5006/')
		apps = {'/': Application(FunctionHandler(self.make_document))}

		self.server = Server(apps, port=8080)
		self.server.start()

	def run(self):
		self.server.io_loop.add_callback(self.server.show, "/")
		thread = Thread(target=self.server.io_loop.start)
		thread.start()
		while not self.simulation.is_finished():
			if self.start:
				time.sleep(0.1)
				yield self.env.timeout(1)

	def make_document(self, doc):
		# source = ColumnDataSource({'x': [], 'y': [], 'color': []})
		# x = []
		# y = []

		colors = ["#75968f", "#a5bab7", "#c9d9d3", "#e2e2e2", "#dfccce", "#ddb7b1", "#cc7878", "#933b41", "#550b1d"]
		mapper = LinearColorMapper(palette=colors, low=0, high=1)
		button = Button(label="Start", button_type="success")
		# create three plots

		plot = {
			'time': [self.env.now],
			'running': [len(self.simulation.cluster.running_tasks)],
			'finished': [len(self.simulation.cluster.finished_tasks)]
		}

		plotdata = ColumnDataSource(plot)

		def update_start():
			if not self.start:
				self.start = True

		p = figure(plot_width=400, plot_height=400)
		p.line(x='time', y='running', legend_label='Running tasks', alpha=0.2, line_width=3, color='navy', source=plotdata)
		p.line(x='time', y='finished', legend_label='Finished tasks', alpha=0.8, line_width=2, color='orange', source=plotdata)
		p.xaxis.axis_label = 'Time'
		p.yaxis.axis_label = 'Tasks'

		def update_plot():
			if self.env:
				updata = {
					'time': [self.env.now],
					'running': [len(self.simulation.cluster.running_tasks)],
					'finished': [len(self.simulation.cluster.finished_tasks)]
				}
				plotdata.stream(updata)

		"""
		SETUP TABLE 
		"""
		columns = [
			TableColumn(field="name", title="Name"),
			TableColumn(field="start", title="Start Time"),
			TableColumn(field="demand", title="No. Antennas"),
			TableColumn(field="duration", title="Observation Length"),
			TableColumn(field="running", title="Running Status")
		]

		sourcedata = dict(name=[], start=[], duration=[], demand=[], running=[])
		tablesource = ColumnDataSource(data=sourcedata)
		for observation in self.simulation.telescope.observations:
			sourcedata['name'].append(observation.name)
			sourcedata['start'].append(observation.start)
			sourcedata['duration'].append(observation.duration)
			sourcedata['demand'].append(observation.demand)
			sourcedata['running'].append(observation.status)
		data_table = DataTable(source=tablesource, columns=columns, width=600, height=280)

		def update_table():
			if self.env:
				for i, observation in enumerate(self.simulation.telescope.observations):
					dictionary = {"running": [(i, observation.status)]}
					tablesource.patch(dictionary)

		doc.add_periodic_callback(update_plot, 500)
		doc.add_periodic_callback(update_table, 500)
		button.on_click(update_start)

		# fig.xgrid.grid_line_color = None
		# fig.x_range.start = 0
		# fig.x_range.end = 350
		# fig.legend.orientation = "horizontal"
		# fig.legend.location = "top_center"

		doc.title = "TopSim Dashboard"
		doc.add_root(layout([button], [p, data_table]))
		return doc
