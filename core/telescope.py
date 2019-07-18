import simpy

# In addition to the broker and scheduler, we need to add a telescope
# to simulate observations, then 'send' data to the buffer at the end
# of the observation


class Telescope(object):

    """
    The telescope is initialised with a list of observations (similar to how
    the broker is initialised with the tasks and their arrival time).

    The telescope simulates observations by 'allocating' an observation to itself,
    and then calling the timeout() when the observation time has elapsed. Then,
    it 'sends' data to the Buffer object, updating the shared resources there.
    """

    def __init__(self):

        pass

    def run(self):
        pass


class Buffer(object):

    def __init__(self):
        pass

    def run(self):
        pass