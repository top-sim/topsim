from setuptools import setup, find_packages

setup(
    name='topsim',
    version='0.3.0',
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        'networkx>=2.0',
        'matplotlib>=3.0',
        'simpy>=3.0',
        'pandas>=0.20',
        'tables',
        'shadow @ git+https://github.com/myxie/shadow.git'
    ],
    dependency_links=[
        'https://github.com/myxie/shadow/tarball/master#egg=shadow'
    ],

    # package_dir={'': 'shadow'},
    url='https://github.com/top-sim/topsim',
    license='GNU',
    author='Ryan Bunney',
    author_email='ryan.bunney@icrar.org',
    description='Simulation framework'
)
