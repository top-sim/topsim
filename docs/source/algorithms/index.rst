Planning and Scheduling
=======================

.. toctree::
    :maxdepth: 2
    :hidden:

    planning
    scheduling

TopSim's development is motivated by evaluating workflow scheduling
procedures proposed for the Square Kilometre Array Science Data
Processor. The framework uses early-binding: resources are pre-allocated
to a workflow before its tasks execute (e.g. SLURM-style batch processing).

The :doc:`planning` model generates a static workflow plan from an
observation's workflow DAG. The :doc:`scheduling` algorithm dynamically
allocates tasks from that plan to cluster machines at each timestep.

For step-by-step guidance on implementing custom planning and scheduling
algorithms, see :ref:`user-defined`.
