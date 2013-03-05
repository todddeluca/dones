
import os
from setuptools import setup, find_packages

setup(
    name = 'dones',
    version = '0.2.0',
    license = 'MIT',
    description = 'Track what keys are "done" using MySQL to implement a '
                  'simple key-store.',
    long_description = open(os.path.join(os.path.dirname(__file__),
                                         'README.md')).read(),
    keywords = 'mysql keystore batch queue LSF',
    url = 'https://github.com/todddeluca/dones',
    author = 'Todd Francis DeLuca',
    author_email = 'todddeluca@yahoo.com',
    classifiers = ['License :: OSI Approved :: MIT License',
                   'Development Status :: 3 - Alpha',
                   'Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 2.7',
                  ],
    py_modules = ['dones'],
    install_requires = ['mysql-python'],
)

