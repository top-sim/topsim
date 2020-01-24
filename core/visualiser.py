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
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure, ColumnDataSource
import json
import numpy as np
import pandas as pd
from bokeh.driving import count
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, Select, Slider
from bokeh.plotting import curdoc, figure
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Slider
from bokeh.models import BasicTicker, ColorBar, ColumnDataSource, LinearColorMapper, PrintfTickFormatter
from bokeh.plotting import figure
from bokeh.models.widgets import Button, DataTable, TableColumn
from bokeh.server.server import Server
from bokeh.themes import Theme
from threading import Thread
from bokeh.colors import color
from bokeh.io import curdoc
from bokeh.layouts import column, row, layout
from bokeh.models import ColumnDataSource, PreText, Select
from bokeh.plotting import figure
from bokeh.palettes import Spectral6
from bokeh.transform import factor_cmap
from bokeh.sampledata.sea_surface_temperature import sea_surface_temperature
from flask import Flask, render_template
from bokeh.embed import server_document
from tornado.ioloop import IOLoop
from bokeh.transform import transform


class Visualiser(object):
	def __init__(self, simulation):
		self.simulation = simulation
		self.env = self.simulation.env
		self.events = []
		self.not_started = True
		self.start = False
		print('Opening Bokeh application on http://localhost:5006/')
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
		sourcedata = dict(name=[], start=[], duration=[], demand=[],running=[])
		for observation in self.simulation.telescope.observations:
			sourcedata['name'].append(observation.name)
			sourcedata['start'].append(observation.start)
			sourcedata['duration'].append(observation.duration)
			sourcedata['demand'].append(observation.demand)
			sourcedata['running'].append(observation.running)

		plot = { 'time': [self.env.now],
				'running': [len(self.simulation.cluster.running_tasks)],
				'finished': [len(self.simulation.cluster.finished_tasks)],
				'waiting': [len(self.simulation.cluster.waiting_tasks)]}

		source = ColumnDataSource(data=sourcedata)
		plotdata = ColumnDataSource(plot)

		def update_start():
			if not self.start:
				self.start = True

		p = figure(plot_width=400, plot_height=400)
		p.line(x='time', y='running', alpha=0.2, line_width=3, color='navy', source=plotdata)
		p.line(x='time', y='finished', alpha=0.8, line_width=2, color='orange', source=plotdata)
		p.line(x='time',y='waiting',alpha=0.5, line_width=2, color='green',source=plotdata)

		columns = [
			TableColumn(field="name", title="Name"),
			TableColumn(field="start", title="Start Time"),
			TableColumn(field="demand", title="No. Antennas"),
			TableColumn(field="duration", title="Observation Length"),
			TableColumn(field="running", title="Running Status")
		]
		data_table = DataTable(source=source, columns=columns, width=600, height=280)

		def update():
			if self.env:
				updata = {'time':[self.env.now],
								'running':[len(self.simulation.cluster.running_tasks)],
								'finished':[len(self.simulation.cluster.finished_tasks)],
								'waiting':[len(self.simulation.cluster.waiting_tasks)]
							}
				plotdata.stream(updata)

		# def table_update():
		# 	if self.env:
		# 		tmp = dict(name=[], start=[], duration=[], demand=[], running=[])
		# 		for observation in self.simulation.telescope.observations:
		# 			tmp['name'].append(observation.name)
		# 			tmp['start'].append(observation.start)
		# 			tmp['duration'].append(observation.duration)
		# 			tmp['demand'].append(observation.demand)
		# 			tmp['running'].append(observation.running)
		# 		source.stream(tmp)

		doc.add_periodic_callback(update, 500)
		# doc.add_periodic_callback(table_update,500)
		button.on_click(update_start)

		# fig.xgrid.grid_line_color = None
		# fig.x_range.start = 0
		# fig.x_range.end = 350
		# fig.legend.orientation = "horizontal"
		# fig.legend.location = "top_center"

		doc.title = "Now with live updating!"
		doc.add_root(layout([button], [p,data_table]))
		return doc
