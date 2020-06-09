import os
from os.path import dirname, join

import core
#import core.filebrowser
#import core.pbm

from core import admin

from core.settings.local import dbaccess, MY_SECRET_KEY, LOG_ROOT

ALLOWED_HOSTS = [
    ### cern.ch
    '.cern.ch',  # Allow domain and subdomains
    '.cern.ch.',  # Also allow FQDN and subdomains
    ### bigpanda.cern.ch
    'bigpanda.cern.ch',  # Allow domain and subdomains
    'bigpanda.cern.ch.',  # Also allow FQDN and subdomains
    ### pandawms.org
    '.pandawms.org',  # Allow domain and subdomains
    '.pandawms.org.',  # Also allow FQDN and subdomains

    '127.0.0.1', '.localhost'
]

### VIRTUALENV
VIRTUALENV_PATH = '/data/virtualenv37'

### WSGI
#WSGI_PATH = VIRTUALENV_PATH + '/pythonpath'

### DB_ROUTERS for atlas's prodtask
DATABASE_ROUTERS = [\
    'core.dbrouter.ProdMonDBRouter', \
    'core.pbm.dbrouter.PandaBrokerageMonDBRouter', \
]
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))




TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            join(dirname(core.__file__), 'templates'),
            join(dirname(admin.__file__), 'templates'),
            join(dirname(core.__file__), 'templates'),
            join(dirname(core.filebrowser.__file__), 'templates'),
            join(dirname(core.pbm.__file__), 'templates'),

        ],
        'OPTIONS': {
            'context_processors': [
                # Insert your TEMPLATE_CONTEXT_PROCESSORS here or use this
                # list if you haven't customized them:
                'django.contrib.auth.context_processors.auth',
                'django.template.context_processors.debug',
                'django.template.context_processors.i18n',
                'django.template.context_processors.media',
                'django.template.context_processors.static',
                'django.template.context_processors.tz',
                'django.contrib.messages.context_processors.messages',
            ],
            'loaders':[
                'django.template.loaders.filesystem.Loader',
                'django.template.loaders.app_directories.Loader',
            ],
            'libraries':{
                'common_tags': 'core.templatetags.common_tags',

                },
        },
    },
]

MEDIA_ROOT = join(BASE_DIR, 'media')
STATIC_URL_BASE = '/static/'
MEDIA_URL_BASE = '/media/'

STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(BASE_DIR, 'static'),
#    join(dirname(core.__file__), 'static'),
)
# Make this unique, and don't share it with anybody.
SECRET_KEY = MY_SECRET_KEY

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases
# DATABASES = {
# #    'default': {
# #        'ENGINE': 'django.db.backends.', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
# #        'NAME': '',                      # Or path to database file if using sqlite3.
# #        'USER': '',                      # Not used with sqlite3.
# #        'PASSWORD': '',                  # Not used with sqlite3.
# #        'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
# #        'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
# #    }
#     'default': defaultDatabase
# }
DATABASES = dbaccess

### URL_PATH_PREFIX for multi-developer apache/wsgi instance
### on EC2: URL_PATH_PREFIX = '/bigpandamon' or URL_PATH_PREFIX = '/developersprefix'
#URL_PATH_PREFIX = '/core'
#URL_PATH_PREFIX = '/twrpmcore'
#URL_PATH_PREFIX = '/core'
URL_PATH_PREFIX = ''
### on localhost:8000: URL_PATH_PREFIX = '/.'
#URL_PATH_PREFIX = ''
MEDIA_URL = URL_PATH_PREFIX + MEDIA_URL_BASE
STATIC_URL = URL_PATH_PREFIX + STATIC_URL_BASE


FILTER_UI_ENV = {
    ### default number of days of shown jobs active in last N days
    'DAYS': 30,
    ### default number of days for user activity of shown jobs active in last N days
    'USERDAYS': 3,
    ### max number of days of shown jobs active in last N days
    'MAXDAYS': 300,
    ### max number of days for user activity of shown jobs active in last N days
    'USERMAXDAYS': 60,
    ### default number of hours of shown jobs active in last N hours
    'HOURS': 2,
    ### wildcard for string pattern in filter form
    'WILDCARDS': ['*'],
    ### wildcard for integer interval in filter form
    'INTERVALWILDCARDS': [':'],
    ###
    'EXPAND_BUTTON': {
        "mDataProp": None,
        "sTitle": "Details",
        "sClass": "control center",
        "bVisible": True,
        "bSortable": False,
        "sDefaultContent": '<img src="' + STATIC_URL + '/images/details_open.png' + '">',
        },
}
#DEBUG=True
#LOG_ROOT = '/data/bigpandamon_virtualhosts/core/logs'
#LOG_ROOT = '/data/wenaus/logs'
#LOG_ROOT = '/afs/cern.ch/user/a/aaleksee/panda-bigmon-core/log'

LOG_SIZE = 1000000000
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'logging.NullHandler',
        },
        'logfile-django': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.django",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-bigpandamon': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.bigpandamon",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-info': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.info",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'full',
        },
        'logfile-error': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.error",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-viewdatatables': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.viewdatatables",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-rest': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.rest",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-api_reprocessing': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.api_reprocessing",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-filebrowser': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.filebrowser",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-pbm': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.pbm",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-template': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.template",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'social': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.social",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class':'logging.StreamHandler',
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'django': {
            'handlers':['logfile-django','logfile-error'],
            'propagate': True,
            'level':'DEBUG',
        },
        'django.template': {
            'handlers': ['logfile-template'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'django_datatables_view': {
            'handlers':['logfile-viewdatatables'],
            'propagate': True,
            'level':'DEBUG',
        },
        'rest_framework': {
            'handlers':['logfile-rest'],
            'propagate': True,
            'level':'DEBUG',
        },
        'bigpandamon': {
            'handlers': ['logfile-bigpandamon', 'logfile-info', 'logfile-error'],
            'level': 'DEBUG',
        },
        'bigpandamon-error': {
            'handlers': ['logfile-error'],
            'level': 'ERROR',
        },
        'api_reprocessing':{
            'handlers': ['logfile-api_reprocessing'],
            'level': 'DEBUG',
        },
        'bigpandamon-filebrowser':{
            'handlers': ['logfile-filebrowser'],
            'level': 'DEBUG',
        },
        'bigpandamon-pbm':{
            'handlers': ['logfile-pbm'],
            'level': 'DEBUG',
        },
        'social':{
            'handlers': ['logfile-error','social'],
            'level': 'DEBUG',
            'propagate': True,
        },
    },
    'formatters': {
        'full': {
            'format': '{asctime} {module} {filename}:{lineno:d} {funcName} pid{process:d} {levelname} {message}',
            'style': '{',
        },
        'verbose': {
            'format': '%(asctime)s %(module)s %(name)-1s:%(lineno)d %(levelname)-5s %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(name)-1s:%(lineno)d %(message)s'
        },
    },
    'logfile': {
        'level':'DEBUG',
        'class':'logging.handlers.RotatingFileHandler',
        'filename': LOG_ROOT + "/logfile",
        'maxBytes': 10000000,
        'backupCount': 5,
        'formatter': 'verbose',
    },
}

# SOCIAL_AUTH_FIELDS_STORED_IN_SESSION = ['state']
# SESSION_COOKIE_SECURE = False

ENV = {
    ### Application name
    'APP_NAME': "PanDA Monitor",
    ### Page title default
    'PAGE_TITLE': "PanDA Monitor",
    ### Menu item separator
    'SEPARATOR_MENU_ITEM': "         ",
    ### Navigation chain item separator
    'SEPARATOR_NAVIGATION_ITEM': "   &#187;   ",
}
