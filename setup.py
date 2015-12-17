#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import os

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="Babywalk",
    version="0.1",
    description="docker wrapper around wget",
    author="László Nagy",
    author_email="rizsotto@gmail.com",
    license='LICENSE.txt',
    url='https://github.com/rizsotto/Babywalk',
    long_description=open('README.md').read(),
    scripts=['bin/seed', 'bin/crawl', 'bin/report'],
    packages=['libbabywalk'],
    install_requires=required,
    test_suite = 'nose.collector'
)
