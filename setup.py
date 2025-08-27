import sys

from setuptools import setup

if sys.version_info < (3, 11):
    raise RuntimeError("requires Python 3.11+")
# Run with
# python3 setup.py sdist bdist_wheel
setup()
