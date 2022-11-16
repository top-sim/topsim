
Configuration
=============

.. toctree::
    :maxdepth: 3
    :hidden:

    simulation
    workflow

.. currentmodule:: topsim.core

What needs configuring?
------------------------

The following actors require configuration, which is parsed by the :py:class:`~config.Config` module when a simulation is initiated.

We use the configuration that supported the :ref:`model-overview <model-overview>` discussion in :ref:`getting-started <getting-started>`.

* Instrument (which also includes the observation plan)
* Cluster
* Buffer
* Timestep

It is also necessary to generate and reference the workflows that are being scheduled for each observation (as appears in the workflow plan).



