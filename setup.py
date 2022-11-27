#!/usr/bin/env python

from setuptools import setup

with open('README.md') as file:
    long_description = file.read()

setup(
    name='lakefs-playground-utils',
    version='0.0.11',
    description='lakeFS Playground Utilities',
    author='Or Tzabary',
    author_email='or.tzabary@treeverse.io',
    url='https://github.com/treeverse/lakefs-playground-utils',
    license='Apache 2.0',
    keywords='lakefs playground',
    platforms=['any'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[
        'fsspec>=2021.11.0',
        'requests>=2.0.0',
        'lakefs-client>=0.85.0',
        'email-validator==1.3.0',
    ],
)
