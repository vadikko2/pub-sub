#!/usr/bin/env python3

from setuptools import setup

DescriptionLong = """/
INT KOLOS-M STATUS DAEMON
"""

version_file = open("VERSION")
pkg_version = version_file.read().strip()

# setup parameters
setup(
    name='int-status-daemons',
    version=pkg_version,
    description='INT KOLOS-M STATUS DAEMON',
    author="Kozyrevskii Vadim",
    author_email="kozyrevskii.vadim@int.spb.ru",
    long_description=DescriptionLong,
    project_urls={
        "INT": "http://int.spb.ru"
    },
    classifiers=[
        "Programming Language :: Python :: 3"
    ]
)
