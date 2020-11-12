.. _actors:

******
Actors
******

Buffer
======

The buffer is a core-component of the Operations Simlator model. In this
scenario, we split the buffer into two components, the Hot and Cold buffer,
as this mirrors the use case for the SKA. Additionally, it makes sense to
separate the real-time streaming timeline from the
post-observation/instrumentation workflow processing.

The Buffer operates within it's own process loop, like the Telescope,
Scheduler, and Cluster. The pseudo-code for this is as follows:

Interacting with the Buffer
----------------------------
Buffer interaction happen primarily through the Scheduler and Cluster.

The cluster and scheduler both have access to the Buffer object, and are able
to invoke processes on it.




