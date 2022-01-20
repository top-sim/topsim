# Test Data

This directory contains the data used to test the functionality of TopSim. Some of these tests are standard unit tests for individual functions/methods for actors; others are integration tests that confirm the behaviour between actors during the simulation. These tests act as confirmation that a) communication between actors is functional, and b) the expected schedules that are planned or dynamically allocated are allocated correctly during the simulation. 

For example, the following things may be tested during integration testing: 
* A HEFT-planned observation workflow will complete within the time it was scheduled for + the offset time from the observation and data transfer
* If workflows are currently under observation.     