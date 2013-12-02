#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(name='RepCRec',
		version='1.0',
		description='Replicated Concurrency Control and Recovery',
		author='Brandon Reiss',
		author_email='blr246@nyu.edu',
		url='https://github.com/blr246/repcrec',
		packages=find_packages(),
		scripts=[
			'bin/repcrec',
			]
		)
