#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="Babywalk",
    version="0.1",
    description="docker wrapper around wget",
    author="László Nagy",
    author_email="rizsotto@gmail.com",
    license='LICENSE.txt',
    url='https://github.com/rizsotto/Babywalk',
    long_description=open('README.md').read(),
    scripts=['bin/crawl'],
    install_requires=["boto3"]
)
