import os, core, logging
from os.path import dirname, join

from core import filebrowser, admin
from core.settings.local import MY_SECRET_KEY, LOG_ROOT

_logger = logging.getLogger('bigpandamon')

ALLOWED_HOSTS = [
    # cern.ch
    '.cern.ch',  # Allow domain and subdomains
    '.cern.ch.',  # Also allow FQDN and subdomains
    # bigpanda.cern.ch
    'bigpanda.cern.ch',  # Allow domain and subdomains
    'bigpanda.cern.ch.',  # Also allow FQDN and subdomains
    # local
    '127.0.0.1',
    '.localhost'
]

if 'BIGMON_HOST' in os.environ:
    ALLOWED_HOSTS.append(os.environ['BIGMON_HOST'])

# IPs of CACHING CRAWLERS if any
CACHING_CRAWLER_HOSTS = ['188.184.185.129', '188.184.116.46', '188.184.90.5']

# IPs of BigPanDAmon backend nodes for DDOS protection script
BIGMON_BACKEND_NODES_IP_LIST = os.environ.get('BIGMON_BACKEND_NODES_IP_LIST', [])

# VIRTUALENV
VIRTUALENV_PATH = os.environ.get('BIGMON_VIRTUALENV_PATH', '/opt/prod')

# WSGI
if 'BIGMON_WSGI_PATH' in os.environ:
    WSGI_PATH = os.environ['BIGMON_WSGI_PATH']

# ASGI
if 'BIGMON_ASGI_PATH' in os.environ:
    ASGI_PATH = os.environ['BIGMON_ASGI_PATH']

# VO
if 'BIGMON_VO' in os.environ:
    MON_VO = os.environ['BIGMON_VO']

# Authentication providers, supported: ['cern', 'google', 'github', 'indigoiam']
if 'BIGMON_AUTH_PROVIDER_LIST' in os.environ and os.environ['BIGMON_AUTH_PROVIDER_LIST']:
    AUTH_PROVIDER_LIST = os.environ['BIGMON_AUTH_PROVIDER_LIST'].split(',')

# PanDA server URL
PANDA_SERVER_URL = os.environ.get('PANDA_SERVER_URL', 'https://pandaserver.cern.ch/server/panda')

# OpenSearch
OS_INDEX_PANDA_LOGS = os.environ.get('ES_INDEX_PANDA_LOGS', 'atlas_pandalogs*')
OS_INDEX_JEDI_LOGS = os.environ.get('ES_INDEX_JEDI_LOGS', 'atlas_jedilogs*')
OS_INDEX_PILOT_LOGS = os.environ.get('ES_INDEX_PILOT_LOGS', 'atlas_pilotlogs*')
OS_CA_CERT = os.environ.get('ES_CA_CERT', '/etc/pki/tls/certs/ca-bundle.trust.crt')

# DB_ROUTERS for atlas's prodtask
DATABASE_ROUTERS = [
    'core.dbrouter.ProdMonDBRouter',
]

# name spaces of DB tables per application
DATABASE_NAME_SPACES = {
    'bigpandamon': 'ATLAS_PANDABIGMON',
    'pandajob': 'ATLAS_PANDA',
    'pandaarchjob': 'ATLAS_PANDAARCH',
    'schedresource': 'ATLAS_PANDAMETA',
    'harvester': 'ATLAS_PANDA',
    'jedi': 'ATLAS_PANDA',
    'prodsys': 'ATLAS_DEFT',
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            join(dirname(core.__file__), 'templates'),
            join(dirname(admin.__file__), 'templates'),
            # join(dirname(core.filebrowser.__file__), 'templates'),
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
                'django.template.context_processors.request',
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
)
# Make this unique, and don't share it with anybody.
SECRET_KEY = MY_SECRET_KEY

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases
try:
    from core.settings.local import dbaccess_postgres
except ImportError:
    dbaccess_postgres = None
try:
    from core.settings.local import dbaccess_oracle_atlas
except ImportError:
    dbaccess_oracle_atlas = None
try:
    from core.settings.local import dbaccess_oracle_doma
except ImportError:
    dbaccess_oracle_doma = None

DEPLOYMENT = os.environ.get('BIGMON_DEPLOYMENT', 'ORACLE_ATLAS')

if DEPLOYMENT in ('ORACLE_ATLAS', 'ORACLE_DOMA'):
    try:
        import oracledb
        oracledb.init_oracle_client(config_dir='/etc/tnsnames.ora')
    except oracledb.exceptions.DatabaseError as e:
        _logger.error(f"Failed to initialize Oracle Client: {e}")
    except Exception as e:
        _logger.error(f"An unexpected error occurred: {e}")

PRMON_LOGS_DIRECTIO_LOCATION = None
if DEPLOYMENT == 'ORACLE_ATLAS':
    DB_SCHEMA = 'ATLAS_PANDABIGMON'
    DB_SCHEMA_PANDA = 'ATLAS_PANDA'
    DB_SCHEMA_PANDA_META = 'ATLAS_PANDAMETA'
    DB_SCHEMA_PANDA_ARCH = 'ATLAS_PANDAARCH'
    DB_SCHEMA_IDDS = 'ATLAS_IDDS'
    DATABASES = dbaccess_oracle_atlas
    CRIC_API_URL = 'https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json'
    IDDS_HOST = 'https://iddsserver.cern.ch:443/idds'
    RUCIO_UI_URL = 'https://rucio-ui.cern.ch/'
    LOGS_PROVIDER = os.environ.get('LOGS_PROVIDER', 'rucio')
elif DEPLOYMENT == 'POSTGRES':
    DB_SCHEMA = 'doma_pandabigmon'
    DB_SCHEMA_PANDA = 'doma_panda'
    DB_SCHEMA_PANDA_ARCH = 'doma_pandaarch'
    DB_SCHEMA_PANDA_META = 'doma_pandameta'
    DB_SCHEMA_IDDS = 'doma_idds'
    DATABASES = dbaccess_postgres
    CRIC_API_URL = os.environ.get('CRIC_API_URL', 'https://datalake-cric.cern.ch/api/atlas/pandaqueue/query/?json')
    IDDS_HOST = os.environ.get('IDDS_HOST', 'https://iddsserver.cern.ch:443/idds')
    RUCIO_UI_URL = os.environ.get('RUCIO_UI_URL', '')
    LOGS_PROVIDER = os.environ.get('LOGS_PROVIDER', 'rucio')
    PRMON_LOGS_DIRECTIO_LOCATION = os.environ.get('PRMON_LOGS_DIRECTIO_LOCATION',
                                                  "https://storage.googleapis.com/drp-us-central1-logging"
                                                  "/logs/{queue_name}/PandaJob_{panda_id}")
elif DEPLOYMENT == 'ORACLE_DOMA':
    DB_SCHEMA = 'DOMA_PANDABIGMON'
    DB_SCHEMA_PANDA = 'DOMA_PANDA'
    DB_SCHEMA_PANDA_ARCH = 'DOMA_PANDAARCH'
    DB_SCHEMA_PANDA_META = 'DOMA_PANDAMETA'
    DB_SCHEMA_IDDS = 'DOMA_IDDS'
    DATABASES = dbaccess_oracle_doma
    CRIC_API_URL = 'https://datalake-cric.cern.ch/api/atlas/pandaqueue/query/?json'
    IDDS_HOST = 'https://aipanda015.cern.ch:443/idds'
    RUCIO_UI_URL = os.environ.get('RUCIO_UI_URL', '')
    LOGS_PROVIDER = os.environ.get('LOGS_PROVIDER', 'cric')
    PRMON_LOGS_DIRECTIO_LOCATION = "https://storage.googleapis.com/drp-us-central1-logging/logs/{queue_name}/PandaJob_{panda_id}"

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

# set default datetime format for datetime.datetime.strftime()
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
if 'ORACLE' in DEPLOYMENT:
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
elif 'POSTGRES' in DEPLOYMENT:
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
elif 'MYSQL' in DEPLOYMENT:
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%SZ"

# max number of items in IN (*,*..) query, if more - use tmp table.
DB_N_MAX_IN_QUERY = 100

CACHES = {
    "default": {
        'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
        'LOCATION': f'"{DB_SCHEMA}"."djangocache"',
        'TIMEOUT': 60*60*24*2,  # 48 hours
        'OPTIONS': {
            'MAX_ENTRIES': 100000000,
        }
    }
}

SESSION_COOKIE_AGE = 60*60*24  # 24 hours
SESSION_API_CALL_AGE = 60*30  # 10 minutes

# URL_PATH_PREFIX for multi-developer apache/wsgi instance
URL_PATH_PREFIX = ''

MEDIA_URL = URL_PATH_PREFIX + MEDIA_URL_BASE
STATIC_URL = URL_PATH_PREFIX + STATIC_URL_BASE

CSP_DEFAULT_SRC = ("'self'", "https://cdnjs.cloudflare.com", "'unsafe-eval'")
CSP_STYLE_SRC = ("'self'", "'unsafe-inline'", "https:")
CSP_CONNECT_SRC = ("'self'", "https:", "wss:", "*.cern.ch")
CSP_IMG_SRC = ("'self'", "https:", "http:", "data:", )
CSP_FRAME_SRC = ("'self'", "https://*.cern.ch")


FILTER_UI_ENV = {
    # default number of days of shown jobs active in last N days
    'DAYS': 30,
    # default number of days for user activity of shown jobs active in last N days
    'USERDAYS': 3,
    # max number of days of shown jobs active in last N days
    'MAXDAYS': 300,
    # max number of days for user activity of shown jobs active in last N days
    'USERMAXDAYS': 60,
    # default number of hours of shown jobs active in last N hours
    'HOURS': 2,
    # wildcard for string pattern in filter form
    'WILDCARDS': ['*'],
    # wildcard for integer interval in filter form
    'INTERVALWILDCARDS': [':'],
    #
    'EXPAND_BUTTON': {
        "mDataProp": None,
        "sTitle": "Details",
        "sClass": "control center",
        "bVisible": True,
        "bSortable": False,
        "sDefaultContent": '<img src="' + STATIC_URL + '/images/details_open.png' + '">',
        },
}

# logging settings. The min level by default is INFO, if debugging it is lowered to DEBUG
LOG_LEVEL = os.environ.get('BIGMON_LOG_LEVEL', 'INFO')
try:
    from core.settings.local import DEBUG
except ImportError:
    DEBUG = False
if DEBUG is True:
    LOG_LEVEL = 'DEBUG'

LOG_SIZE = 100000000
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        },
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        },
    },
    'handlers': {
        'null': {
            'level': LOG_LEVEL,
            'class': 'logging.NullHandler',
        },
        'logfile-django': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.django",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-bigpandamon': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
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
        'logfile-filebrowser': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.filebrowser",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-art': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.art",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'logfile-template': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.template",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'social': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.social",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'panda-client': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_ROOT + "/logfile.panda.client",
            'maxBytes': LOG_SIZE,
            'backupCount': 2,
            'formatter': 'verbose',
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'logging.StreamHandler',
        },
        'console': {
            'level': LOG_LEVEL,
            'filters': ['require_debug_true'],
            'class': 'logging.StreamHandler',
            'formatter': 'full'
        },
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins', 'logfile-django'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'django': {
            'handlers': ['logfile-django', 'logfile-error'],
            'propagate': True,
            'level': LOG_LEVEL,
        },
        'django.template': {
            'handlers': ['logfile-template'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'bigpandamon': {
            'handlers': ['logfile-bigpandamon', 'logfile-info', 'logfile-error', 'console'],
            'level': LOG_LEVEL,
        },
        'bigpandamon-error': {
            'handlers': ['logfile-error'],
            'level': 'ERROR',
        },
        'bigpandamon-art': {
            'handlers': ['logfile-art', 'logfile-error', 'console'],
            'level': LOG_LEVEL,
        },
        'bigpandamon-filebrowser': {
            'handlers': ['logfile-filebrowser', 'logfile-error', 'logfile-info', 'console'],
            'level': LOG_LEVEL,
        },
        'panda.client': {
            'handlers': ['panda-client'],
            'level': 'DEBUG',
        },
        'social': {
            'handlers': ['logfile-error', 'social'],
            'level': LOG_LEVEL,
            'propagate': True,
        }
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
        'level': LOG_LEVEL,
        'class': 'logging.handlers.RotatingFileHandler',
        'filename': LOG_ROOT + "/logfile",
        'maxBytes': LOG_SIZE,
        'backupCount': 5,
        'formatter': 'verbose',
    },
}

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

