The anatomy of a simulation
===========================

.. currentmodule:: topsim.core

Simulations are started from a simulation object, with the main actors
initialised prior to a simulations;

* :py:class:`~instrument.Instrument`
* Scheduler
* :py:class:`~cluster.Cluster`
* Buffer
* Monitor

There are a number of requirements that must be True for an observation to
commence on the telescope:

Simulation Setup and configuration
==================================

