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

from bokeh.driving import count
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, Select, Slider
from bokeh.plotting import curdoc, figure
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Slider
from bokeh.plotting import figure
from bokeh.server.server import Server
from bokeh.themes import Theme
from threading import Thread

from bokeh.io import curdoc
from bokeh.layouts import column, row,layout
from bokeh.models import ColumnDataSource, PreText, Select
from bokeh.plotting import figure

from bokeh.sampledata.sea_surface_temperature import sea_surface_temperature
from flask import Flask, render_template
from bokeh.embed import server_document
from tornado.ioloop import IOLoop


class Visualiser(object):
	def __init__(self, simulation):
		self.simulation = simulation
		self.env = self.simulation.env
		self.events = []
		self.not_started = True

		print('Opening Bokeh application on http://localhost:5006/')
		apps = {'/': Application(FunctionHandler(self.make_document))}

		self.server = Server(apps, port=8080)
		self.server.start()

	def run(self):
		self.server.io_loop.add_callback(self.server.show, "/")
		thread = Thread(target=self.server.io_loop.start)
		thread.start()
		while not self.simulation.is_finished():
			time.sleep(0.1)
			yield self.env.timeout(1)

	def make_document(self, doc):
		source = ColumnDataSource({'x': [], 'y': [], 'color': []})
		x = []
		y = []

		x = list(range(11))
		y0 = x
		y1 = [10 - i for i in x]
		y2 = [abs(i - 5) for i in x]

		# create three plots
		s1 = figure(background_fill_color="#fafafa")
		s1.circle(x, y0, size=12, alpha=0.8, color="#53777a")

		s2 = figure(background_fill_color="#fafafa")
		s2.triangle(x, y1, size=12, alpha=0.8, color="#c02942")

		s3 = figure(background_fill_color="#fafafa")
		s3.square(x, y2, size=12, alpha=0.8, color="#d95b43")
		plots = row(s1,s2,s3)

		def update():
			if self.env:
				new = {'x': [self.env.now], 'y': [self.env.now],
					'color': ['blue']}
				source.stream(new)

		doc.add_periodic_callback(update, 500)

		fig = figure(title='Simulation playback!', sizing_mode='scale_height',
					x_range=[0, 310], y_range=[0, 310])
		fig.line(source=source, x='x', y='y', color='red')  # size=10)


		doc.title = "Now with live updating!"
		doc.add_root(layout([fig],[plots]))
		return doc
