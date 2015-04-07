#!/usr/bin/env python

import setuptools
import os
import shutil
import json
import glob

HERE = os.path.abspath(os.path.dirname(__file__))
here = lambda fn: os.path.join(HERE, fn)

_PACKAGE_NAME = "spate"
_VERSION = json.load(open(here("VERSION"), "rU"))
_DEPENDENCIES = open(here("DEPENDENCIES"), "rU").readlines()
_PACKAGED_FILES = [
    "DEPENDENCIES",
    "VERSION",
    "CHANGELOG",
]

with open(here("MANIFEST.in"), "w") as manifest:
    for fn in _PACKAGED_FILES:
        manifest.write("include " + fn + "\n")

try:
    setuptools.setup(
        # Package description
        name = _PACKAGE_NAME,
        description = "Lightweight file-based workflow management system",
        version = "%(changeset_latest_tag)s%(changeset_local_revision)s" % _VERSION,
        author = "Aurelien Mazurie",
        author_email = "ajmazurie@oenone.net",

        # Package requirements
        install_requires = _DEPENDENCIES,

        # Package components
        packages = [
            _PACKAGE_NAME,
            _PACKAGE_NAME + ".exporters",
        ],
        package_dir = {
            _PACKAGE_NAME: "lib"
        },
        package_data = {
            _PACKAGE_NAME: _PACKAGED_FILES
        },
        scripts = glob.glob("bin/*"),
    )
finally:
    os.remove(here("MANIFEST.in"))
