from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure, ColumnDataSource
import json
import numpy as np
import random


def make_document(doc):
	source = ColumnDataSource({'x': [], 'y': [], 'color': []})
	fp = open('../sim.trace')
	trace = json.load(fp)
	cpu_capacity = []
	for timestamp in trace:
		# print(timestamp['cluster_state']['machine_states'][0])
		# get the CPU_id and usage value
		id_list = [x['cpu_capacity'] for x in timestamp['cluster_state']['machine_states']]
		cpu_capacity.append(id_list)
	np_timestamp = np.array(cpu_capacity)
	np_timestamp = np_timestamp.transpose()
	x = list([x for x in range(np_timestamp.shape[1])])
	y = list(np_timestamp[0])
	print(x)

	def update():
		print(y[0] // 2)
		tmp = x.pop(0)
		ytmp = 50
		if tmp%2 is 0:
			ytmp= 100*tmp
		new = {'x': [tmp], 'y': [ytmp // (tmp + 1)],
			'color': [random.choice(['red', 'blue', 'green'])]}
		source.stream(new)

	doc.add_periodic_callback(update, 500)

	fig = figure(title='Simulation playback!', sizing_mode='scale_height',
				 x_range=[0, 100], y_range=[0, 100])
	fig.line(source=source, x='x', y='y', color='red')  # size=10)

	doc.title = "Now with live updating!"
	doc.add_root(fig)


apps = {'/': Application(FunctionHandler(make_document))}

server = Server(apps, port=5000)
server.start()

if __name__ == '__main__':
	print('Opening Bokeh application on http://localhost:5006/')

	server.io_loop.add_callback(server.show, "/")
	server.io_loop.start()
