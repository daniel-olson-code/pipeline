"""Builds multiple Cython scripts using setuptools.

This script automates the process of building multiple Cython files using
setuptools. It creates a temporary setup.py file for each Cython script
and runs the build command.

"""

import subprocess
import shlex

# List of Cython scripts to be built
scripts = ['c_pipeline.pyx', 'c_worker.pyx', 'c_bucket.pyx']

# Template for the setup.py script
setup_script = '''from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("{script}"),
)'''

# Command to run the setup.py script
cmd = 'python setup.py build_ext --inplace'

# Iterate through each Cython script
for script in scripts:
    # Write the setup script for the current Cython file
    with open('setup.py', 'w') as f:
        f.write(setup_script.format(script=script))

    # Run the build command
    subprocess.run(shlex.split(cmd))