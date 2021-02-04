from setuptools import setup, find_packages

setup(
	name='topsim',
	version='0.1',
	packages=find_packages(exclude=("test",)),
	install_requires=[
		'networkx>=2.0',
		'matplotlib>=3.0',
		'simpy>=3.0',
		'pandas>=0.20',
		'shadow @ git+https://github.com/myxie/shadow.git'
	],
	dependency_links=[
		'http://github.com/myxie/shadow/tarball/master#egg=shadow'
	],

	# package_dir={'': 'shadow'},
	url='https://github.com/top-sim/topsim',
	license='GNU',
	author='Ryan Bunney',
	author_email='ryan.bunney@icrar.org',
	description='Simulation framework'
)
