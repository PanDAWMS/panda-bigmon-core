"""
WSGI config for bigpandamon project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/howto/deployment/wsgi/

further doc: http://thecodeship.com/deployment/deploy-django-apache-virtualenv-and-mod_wsgi/

"""

import os
import sys
import site
import logging
from os.path import join, pardir, abspath, dirname, split
from django.core.wsgi import get_wsgi_application

_logger = logging.getLogger('bigpandamon')

try:
    from core.settings.config import VIRTUALENV_PATH, WSGI_PATH
    virtualenvPath = VIRTUALENV_PATH
    path = WSGI_PATH
except ImportError:
    virtualenvPath = '/opt/prod'
    path = virtualenvPath + '/pythonpath'
    _logger.exception("Something went wrong with import of WSGI_PATH from settings.")
    _logger.exception("Staying with default path: {}".format(path))

# Add the site-packages of the chosen virtualenv to work with
site.addsitedir(virtualenvPath + '/lib/python3.10/site-packages')

# Add the app's directory to the PYTHONPATH
sys.path.append(path)
sys.path.append(path + '/pythonpath')

# django settings module
DJANGO_SETTINGS_MODULE = '%s.%s' % (split(abspath(dirname(__file__)))[1], 'settings')
os.environ['DJANGO_SETTINGS_MODULE'] = DJANGO_SETTINGS_MODULE

# pythonpath dirs
PYTHONPATH = [
    join(dirname(__file__), pardir),
]

# inject few paths to pythonpath
for p in PYTHONPATH:
    if p not in sys.path:
        sys.path.insert(0, p)

# load config variables from to env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(path, "core", "settings", "config_vars.env"))
    print(os.environ)
except ImportError:
    _logger.exception("No config vars were loaded to env")
except:
    _logger.exception("Failed to run load_dotenv()")

# Activate your virtual env
try:
    activate_env = os.path.expanduser(virtualenvPath + '/bin/activate_this.py')
    exec(open(activate_env).read(), dict(__file__=activate_env))
except:
    _logger.exception("Virtual env is not activated")

application = get_wsgi_application()
