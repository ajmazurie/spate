#!/usr/bin/env python

import glob
import setuptools

setuptools.setup(
    name = "spate",
    version = "0.2.0",
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
    url = "https://github.com/ajmazurie/spate",
    download_url = "https://github.com/ajmazurie/spate/archive/0.2.0.zip",
    classifiers = [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Distributed Computing"])
