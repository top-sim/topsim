# Copyright (C) 27/11/19 RW Bunney

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
import simpy


class TestCluster(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def testClusterQueue(self):
		env = simpy.Environment()
		env.process(self.run_env(env))
		env.run(until=20)

	def run_env(self, env):
		i = 0
		while i < 15:
			print(env.now)
			if env.now == 5:
				env.process(self.secret(env))
			i += 1
			yield env.timeout(1)

	def secret(self, env):
		print('Started secret business @ {0}'.format(env.now))
		yield env.timeout(4)
		print('Finished secret business @ {0}'.format(env.now))
