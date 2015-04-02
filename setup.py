#!/usr/bin/env python

import setuptools
import os
import shutil
import json
import glob

HERE = os.path.abspath(os.path.dirname(__file__))
here = lambda fn: os.path.join(HERE, fn)

_VERSION = json.load(open(here("VERSION"), "rU"))

_DEPENDENCIES = open(here("DEPENDENCIES"), "rU").readlines()

_PACKAGED_FILES = [
    "DEPENDENCIES",
    "VERSION",
    "CHANGELOG"
]

setuptools.setup(
    # Package description
    name = "spate",
    description = "Lightweight file-based workflow management system",
    version = "%(changeset_latest_tag)sa%(changeset_local_revision)s" % _VERSION,
    author = "Aurelien Mazurie",
    author_email = "ajmazurie@oenone.net",

    # Package requirements
    install_requires = _DEPENDENCIES,

    # Package components
    packages = [
        "spate"
    ],
    package_dir = {
        "spate": "lib"
    },
    package_data = {
        "spate": _PACKAGED_FILES
    },
    scripts = glob.glob("bin/*"),
)
