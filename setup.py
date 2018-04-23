"""
This file installs the SimManager package.
"""
import re

from setuptools import setup

__author__ = "Anand Subramoney"
__version__ = "1.7.2"


def get_requirements(filename):
    """
    Helper function to read the list of requirements from a file
    """
    dependency_links = []
    with open(filename) as requirements_file:
        requirements = requirements_file.read().strip('\n').splitlines()
    for i, req in enumerate(requirements):
        if ':' in req:
            match_obj = re.match(r"git\+(?:https|ssh|http):.*#egg=(.*)-(.*)", req)
            assert match_obj, "Cannot make sense of url {}".format(req)
            requirements[i] = "{req}=={ver}".format(req=match_obj.group(1), ver=match_obj.group(2))
            dependency_links.append(req)
    return requirements, dependency_links


requirements, dependency_links = get_requirements('requirements.txt')

setup(
    name="SimRecorder",
    version=__version__,
    packages=('simrecorder',),
    author=__author__,
    author_email="anand@igi.tugraz.at",
    description="The Simulation Recorder is a library for recording data for scientific simulations.",
    provides=['simrecorder'],
    install_requires=requirements,
    dependency_links=dependency_links,
)
