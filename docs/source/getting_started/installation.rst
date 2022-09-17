.. _installation:

Getting started
===================

.. toctree::
    :maxdepth: 2
    :hidden:

    model_overview


Installation Requirements
-------------------------

TopSim builds it's discrete event model on the **SimPy** library, and incorporates the **networkx** library for its backend model. The internal data storage uses **pandas**, and numerical and visualisation . In addition, it makes use of common mathematics and visualisation libraries such as **numpy** and **matplotlib**.

Optional
^^^^^^^^

In order to run the :ref:`user-defined <user-defined>` planning and scheduling algorithms in the ``topsim/user``, the Scheduling Algorithms for DAG Workflows (SHADOW) scheduling library is also required.


Installing in Anaconda
-----------------------

The above dependencies (apart from SHADOW) are likely already present if you have an Anaconda installation. To create a ``conda`` environment::

	conda create -n venv python
	source activate venv

It is then possible to run the following to install TopSim locally::

	cd /path/to/topsim
	pip install -e .

Installing in `virtualenv`
--------------------------

Since Python 3.3, is has been possible to create a `virtual environment https://docs.python.org/3/library/venv.html` without installing additional dependencies Python. Once a virtual environment is created::

	cd /path/to/topsim
	pip install -r requirements.txt
	pip install -e.

Installing optional libraries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
As described above, in order to run the provided user-defined planning and scheduling heuristics, it is necessary to use the SHADOW scheduling library. This can be installed using the provided requirements.txt file::

	pip install -r requirements.txt

It is also possible to clone the SHADOW library locally and install it locally, too::

	git clone https://github.com/myxie/shadow
	cd shadow/
	pip install -e .

Verifying installation
----------------------

In the main source directory, it is useful to verify that installation has worked correctly, and libraries are up to date, by using the tests::

    python -m unittest discover

**Note:** In order to pass the unittests, it is necessary to follow `Installing optional libraries`_ first.
