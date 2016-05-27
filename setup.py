#!/usr/bin/env python

import glob
import setuptools

__NAME__ = "spate"
__VERSION__ = "0.2.3"

setuptools.setup(
    name = __NAME__,
    version = __VERSION__,
    description = (
        "Lightweight workflow composition and generation engine"),

    # package author
    author = "Aurelien Mazurie",
    author_email = "ajmazurie@oenone.net",

    # package content
    packages = [
        "spate",
        "spate.io"],
    package_dir = {
        "spate": "lib"},
    scripts = glob.glob("bin/*"),

    # package requirements
    install_requires = [
        "colorama",
        "enum34",
        "networkx",
        "pystache",
        "pyyaml"],

    # package metadata
    url = "https://github.com/ajmazurie/" + __NAME__,
    download_url = "https://github.com/ajmazurie/%s/archive/%s.zip" % (
        __NAME__, __VERSION__),
    classifiers = [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Distributed Computing"])
