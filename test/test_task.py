# Copyright (C) 10/12/19 RW Bunney

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

import os
import unittest
from queue import PriorityQueue

from core.scheduler import Task


class TestTaskClass(unittest.TestCase):
	"""
	Test equivelance and priorities for task class within priority queue
	"""
	def setUp(self):
		self.t1 = Task(1)
		self.t2 = Task(2)
		self.pq = PriorityQueue()
		pass

	def tearDown(self):
		pass

	def testEqualPriority(self):
		# By default, Task priority is 0
		self.pq.put(self.t1)
		self.pq.put(self.t2)
		get_t1 = self.pq.get()
		get_t2 = self.pq.get()

		self.assertEqual(get_t1.id, self.t1.id)
		self.assertEqual(get_t2.id, self.t2.id)

		pass

	def testDifferentPriority(self):
		self.t1.priority = 2
		self.t2.priority = 1

		self.pq.put(self.t1)
		self.pq.put(self.t2)
		# PriorityQueue class preferences smaller over larger; hence, t2 should be popped first
		get_t2 = self.pq.get()
		get_t1 = self.pq.get()

		self.assertEqual(get_t1.id, self.t1.id)
		self.assertEqual(get_t2.id, self.t2.id)

	# TODO Test the order of two independent sets of tasks with different priorities
	# TODO is retained when they are added to the queue

	def testWorkflowExecutionOrderSanity(self):
		pass
