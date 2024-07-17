import subprocess
import shlex


scripts = ['c_pipeline.pyx', 'c_worker.pyx', 'c_bucket.pyx']

setup_script = '''from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("{script}"),
)'''

cmd = 'python setup.py build_ext --inplace'

for script in scripts:
    with open('setup.py', 'w') as f:
        f.write(setup_script.format(script=script))
    subprocess.run(shlex.split(cmd))

