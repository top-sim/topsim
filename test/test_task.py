# Copyright (C) 19/10/20 RW Bunney

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import unittest
import copy
import simpy

from topsim.core.delay import DelayModel
from topsim.core.task import Task, TaskStatus


class TaskInit(unittest.TestCase):

    def setUp(self):
        self.task_id = 'apricot_jam_0_10'
        self.dm = None
        self.est = 0
        self.eft = 11
        self.allocated_machine_id = 'x1'
        self.flops = 10000
        self.pred = []
        
    def test_task_init(self):
        t = Task(
            tid=self.task_id,
            est=self.est,
            eft=self.eft,
            machine_id=self.allocated_machine_id,
            predecessors=None,
            flops=0, task_data=0, io=0,
            delay=self.dm)


class TestTaskDelay(unittest.TestCase):

    def setUp(self):
        self.task_id = 'apricot_jam_0_10'
        # Based on seed, this does not produce a delay
        self.dm_nodelay = DelayModel(0.1, "normal")
        # Based on seed, this does produce a delay
        self.dm_delay = DelayModel(0.3, "normal")
        self.assertEqual(20, self.dm_nodelay.seed)

        self.allocated_machine_id = 'x1'
        self.flops = 10000
        self.pred = []

        self.env = simpy.Environment()

    def testTaskWithOutDelay(self):
        dm = copy.copy(self.dm_nodelay)
        t = Task(
            self.task_id,
            est=0,
            eft=11,
            machine_id=None,
            predecessors=None,
            flops=0, task_data=0, io=0,
            delay=dm)
        # t.est = 0
        # t.eft = 11
        # t.duration = t.eft - t.est
        t.ast = 0
        delay = t._calc_task_delay()
        self.assertEqual(0, delay - t.duration)

    def testTaskWithDelay(self):
        dm = copy.copy(self.dm_delay)
        t = Task(
            self.task_id,
            est=0,
            eft=11,
            machine_id=None,
            predecessors=None,
            flops=0, task_data=0, io=0,
            delay=dm)

        # t.est = 0
        # t.eft = 11
        t.ast = 0
        # t.duration = t.eft - t.est
        delay = t._calc_task_delay()
        self.assertEqual(1, delay - t.duration)

    def testTaskDoWorkWithOutDelay(self):
        dm = copy.copy(self.dm_nodelay)
        t = Task(
            self.task_id,
            est=0,
            eft=11,
            machine_id=None,
            predecessors=None,
            flops=0, task_data=0, io=0,
            delay=dm)
        # t.ast = 0
        t.duration = t.eft - t.est
        self.env.process(t.do_work(self.env, None))
        self.env.run()
        self.assertEqual(11, t.aft)

    # TODO update task with data transfer data
    def testTaskDoWorkWithDelay(self):
        dm = copy.copy(self.dm_delay)
        t = Task(
            self.task_id,
            est=0,
            eft=11,
            machine_id = None,
            predecessors=None,
            flops=0, task_data=0, io=0,
            delay=dm)
        self.env.process(t.do_work(self.env, None))
        self.env.run()
        self.assertEqual(12, t.aft)
        self.assertTrue(t.delay_flag)

class TestTaskRuntimes(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass