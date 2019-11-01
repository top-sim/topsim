Running a TopSim Simulation
===========================
.. code-block:: python

	trace_file = 'sim.trace'
	machines = config.process_machine_config('cluster_config.json')
	observations = config.process_telescope_config('observations.csv')

	# Initialise the simultion environment
	env = simpy.Environment()

	# Set up Actors and Resources
	cluster = Cluster()
	cluster.add_machines(machines)
	algorithm = FifoAlgorithm()
	scheduler = Scheduler(env, algorithm)
	planner = Planner(env, 'heft', 'cluster_config.json')
	buff = Buffer(env)

	max_antennas = 36 # ASKAP
	telescope = Telescope(env, observations, buff, max_arrays, planner)

	# Simulation object activates initial actors and starts the simulation monitoring
	simulation = Simulation(env, telescope, cluster, buff, scheduler, trace_file)
	simulation.init_process()
	env.run()
