.. _installation:

Installation
============

Requirements
------------

TopSim uses **SimPy** for discrete-event simulation, **NetworkX** for
workflow graph representation, **pandas** for data storage, and
**NumPy**/**Matplotlib** for numerical and visualisation support.

The SHADOW scheduling library (required for HEFT/PHEFT planning) is
installed automatically as a dependency.

Installing TopSim
-----------------

.. code-block:: bash

    pip install topsim

For a local development installation:

.. code-block:: bash

    cd /path/to/topsim
    pip install -e .

Verifying the installation
--------------------------

.. code-block:: bash

    topsim version

Or run the test suite:

.. code-block:: bash

    python -m unittest discover
