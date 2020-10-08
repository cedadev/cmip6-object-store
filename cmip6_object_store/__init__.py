# -*- coding: utf-8 -*-

"""Top-level package for cmip6-object-store."""

__author__ = """Ag Stephens"""
__contact__ = "ag.stephens@stfc.ac.uk"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__version__ = "0.1.0"

import logging
import os

from .config import CONFIG

LOG_LEVEL = "INFO"
logging.basicConfig(level=LOG_LEVEL)


for env_var, value in CONFIG["env_vars"].items():
    os.environ[env_var.upper()] = value
