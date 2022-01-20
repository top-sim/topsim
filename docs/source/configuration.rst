.. _configuration:

Simulations
=========================

Observations
------------
As discussed previously, the level of granularity provided to users depends on the focus of the library;
currently, observations are represented in a very high level manner, and  defined by;
their planned start time (``start``), the duration of the observation (``duration``),
how many antennas are to be used during the observation (``demand``), and the location of the related
workflow descrition file (``filename``).

.. code-block:: html

	name,start,duration,demand,filename
	emu,0,10,36,test/emu_spec.json
	dingo,10,15,18,test/dingo_spec.json
	vat,20,30,18,test/vat_spec.json

Cluster
-------
The cluster is initalised using the *shadow* library environment specification JSON format. An example

.. code-block:: json

	{
		"system": {
			"resources": {
				"cat0_m0": {
					"flops": 7.0
				},
				"cat1_m1": {
					"flops": 6.0
				},
				"cat2_m2": {
					"flops": 11.0
				}
			},
			"rates": {
				"cat0": 1.0,
				"cat1": 1.0,
				"cat2": 1.0
			}
		}
	}

Plans
-----
Plans are derived from passing the workflow specification file associated with a workflow to
the *shadow* library, which will use a specified algorithm to generate a static plan. The plan is then captured within
TopSim's Plan object, the class definition of which can be found in ``core.planner``.

