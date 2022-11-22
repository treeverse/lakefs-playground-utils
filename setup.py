from setuptools import setup

setup(
    name='lakefs-playground-utils',
    version='0.0.1',
    install_requires=[
	'fsspec>=2021.11.0',
	'requests>=2.0.0'
	'lakefs-client==0.85.0',
    ],
)
