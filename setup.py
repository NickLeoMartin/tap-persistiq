#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-persistiq',
      version='0.0.1',
      description='Singer.io tap for the Intercom API V2.0',
      author='nickleomartin@gmail.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_persistiq'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'singer-python==5.8.1'
      ],
      entry_points='''
          [console_scripts]
          tap-persistiq=tap_persistiq:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_persistiq': [
              'schemas/*.json',
              'tests/*.py'
          ]
      })
