"""
    Settings package to replace standard django project settings.py. 
    It separates 
    A) base.py: general project configuration, should not change this one
    B) config.py: machine-specific environment config with sensible defaults
    C) local.py: settings to override machine-specific environment config

"""

# Load base configuration for the whole application
from core.settings.base import *

# Load config
from core.settings.config import *

# Load any settings for local development
try:
    from core.settings.local import *
except ImportError:
    pass


import os
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.WE8ISO8859P1'


