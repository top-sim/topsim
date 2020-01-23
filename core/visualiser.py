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
from bokeh.models.widgets import Button
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
		machines = self.simulation.cluster.machines
		machine_ids = [m.id for m in machines]
		color = 'blue'
		tempdf = pd.DataFrame({"machine_ids": machine_ids, "current_time":self.env.now})
		tempdf.columns.name = "machine_ids"
		taskexist = []
		for machine in machines:
			val = 0
			if machine.current_task:
				val = 1
			taskexist.append(val)
		tempdf['taskexist'] = taskexist
		tempdf = tempdf.set_index('current_time')
		df = pd.DataFrame(tempdf.stack(), columns=['taskexist']).reset_index()

		source = ColumnDataSource(df)
		def update_start():
			if not self.start:
				self.start = True

		def update():
			if self.env:
				machines = self.simulation.cluster.machines
				machine_ids = [m.id for m in machines]
				color = 'blue'
				current_time = self.env.now
				intermed = pd.DataFrame({"machine_ids": machine_ids, "current_time": current_time})
				taskexist = []
				for machine in machines:
					val = 0
					if machine.current_task:
						val = 1
					taskexist.append(val)
				intermed['taskexist'] = taskexist
				intermed = intermed.set_index('current_time')
				new = {"index":[self.env.now for m in machine_ids], "machine_ids":machine_ids,"taskexist":taskexist,"current_time":[self.env.now for m in machine_ids]}
				source.stream(new)

		doc.add_periodic_callback(update, 500)
		button.on_click(update_start)

		p = figure(plot_width=800, plot_height=300, title="Simulation",
				x_range=[0,350], y_range=list(reversed(tempdf.columns)),
				toolbar_location=None, tools="", x_axis_location="above")

		p.rect(x="current_time",y="machine_ids", width=1, height=1, source=source,
			line_color=None, fill_color=transform('taskexist', mapper))
		# fig = figure(title='Simulation playback!',plot_height=250,
		# 			x_range=[0,350], y_range=machine_ids)
		# fig.hbar(y="machine_ids", right="time", height=0.9, source=source,
		# 		line_color='white', fill_color=factor_cmap('machines',
		# 												palette=Spectral6,
		# 												factors=machine_ids
		# 												))
		# fig.line(source=source, x='x', y='y', color='blue')  # size=10)
		color_bar = ColorBar(color_mapper=mapper, location=(0, 0),
							 ticker=BasicTicker(desired_num_ticks=len(colors)),
							 formatter=PrintfTickFormatter(format="%d%%"))

		p.add_layout(color_bar, 'right')
		p.axis.axis_line_color = None
		p.axis.major_tick_line_color = None
		p.axis.major_label_text_font_size = "5pt"
		p.axis.major_label_standoff = 0
		p.xaxis.major_label_orientation = 1.0
		# fig.xgrid.grid_line_color = None
		# fig.x_range.start = 0
		# fig.x_range.end = 350
		# fig.legend.orientation = "horizontal"
		# fig.legend.location = "top_center"

		doc.title = "Now with live updating!"
		doc.add_root(layout([button], [p]))
		return doc
