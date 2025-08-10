-----------------
TopSim
-----------------


Telescope OPerations Simulator (TopSim) is a simulation framework built to
evaluate planning and scheduling paradigms for large-scale telescope
operations, such as the Square Kilometre Array (SKA). 

Overview
---------

TopSim follows a discrete event simulation model, using the
`SimPy <https://simpy.readthedocs.io/en/latest/>`_ module as
the foundation for the simulations. 

The context of a TopSim simulation is and end-to-end run-through of a mid-term
observation plan for a (radio) telescope. A simulation contains various
'Actors'; some are initialised and start at the beginning of the simulation;
others are activated by other Actors throughout the duration of the
simulation. The different Actors, their roles, and how they may be invoked,
are discussed in more detail in the section.

The primary motivation of TopSim is to test new models of scheduling and
planning for Radio Astronomy post-processing. As a result, the focus of
TopSim is primarily on the design and implementation of the workflow, plan
scheduling models. This means the relative granularity and expressiveness of
other parts of the system - such as observation specifications - are low. More
discussion on the different levels of abstraction of the different actors and
resources is covered in both the :ref:`model_overview` and
:ref:`configuration`
sections.


Installation
-------------

.. toctree::
    :maxdepth: 3

    getting_started/index
    configuration/index
    algorithms/index
    reference/index