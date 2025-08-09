# Telescope Operations Simulator
[![Python package](https://github.com/top-sim/topsim/actions/workflows/run-unittests.yml/badge.svg)](https://github.com/top-sim/topsim/actions/workflows/run-unittests.yml)[![Coverage Status](https://coveralls.io/repos/github/top-sim/topsim/badge.svg?branch=development)](https://coveralls.io/github/top-sim/topsim?branch=master)

TopSim is a telescope observation and data post-processing simulator, based on
a fork of Robert Lexis' CloudSimPy. The intention of TopSim is to provide an
end-to-end view of telescope observations, data-archival, and the subsequent
processing of observation data products, with a focus on the mid-term timeline
commonly used in telescope semester plans. The main intention of the simulator
is to test new workflow planning and scheduling techniques designed for the
[Square Kilometre Array (SKA)](https://www.skatelescope.org/) and its precursor
telescope, the [Australian Square Kilomere Array Pathfinder
(ASKAP)](https://www.atnf.csiro.au/projects/askap/index.html).

TopSim is being actively developed by [Ryan
Bunney](https://www.icrar.org/people/rbunney/), a PhD Candidate at the
[International Centre for Radio Astronomy Research
(ICRAR)](https://www.icrar.org/), in Perth, Western Australia. 

## Dependencies

TopSim uses the `Simpy` discrete-event simulation framework; in addition to
this, the following packages are necessary to use the full feature set of
TopSim:

* numpy
* Networkx
* matplotlib
* pandas 

TOpSim's tests and sample simulations also use the SHADOW scheduling
 framework, which can be found at https://github.com/myxie/shadow. 

## Installing TOpSIm

It is best to install TOpSim using a virtual environment from `virtualenv
`. Running and installation using the `setup.py` file should be enough
 to ensure that you are able to use the full codebase: 
 
 ```python
python setup.py install
```

To test all requirements are installed, run the tests using: 

```python
python -m unittest discover test
```

## Running your first simulation 

