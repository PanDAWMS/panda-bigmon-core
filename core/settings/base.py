from os.path import dirname, join

from core import common
import core
from core import filebrowser
from core import pbm

ADMINS = (
    ('Sergey Podolsky', 'spadolski@bnl.gov'),
)
MANAGERS = ADMINS

LANGUAGE_CODE = 'en-us'
LANGUAGE_NAME = 'English'
LANGUAGE_NAME_LOCAL = 'English'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Site ID
SITE_ID = 1

STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
#    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)


MIDDLEWARE_CLASSES = (
    'debug_toolbar.middleware.DebugToolbarMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',  # for AJAX POST protection with csrf
    'django.contrib.auth.middleware.AuthenticationMiddleware',
### added
    'django.contrib.auth.middleware.RemoteUserMiddleware',  # for APIs: htcondorapi
### END added
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
#### django-debug-toolbar
#    'debug_toolbar.middleware.DebugToolbarMiddleware',
###
#    'django.middleware.common.CommonMiddleware',
#    'django.contrib.sessions.middleware.SessionMiddleware',
#    'django.middleware.csrf.CsrfViewMiddleware',  # for AJAX POST protection with csrf
#    'django.contrib.auth.middleware.AuthenticationMiddleware',
#    'django.contrib.messages.middleware.MessageMiddleware',
#    # Uncomment the next line for simple clickjacking protection:
#    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'common.urls'

### added
TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(core.common.__file__), 'templates'),
)


# installed apps
INSTALLED_APPS_DJANGO_FRAMEWORK = (
    ### Django framework
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
#### django-debug-toolbar
#    'debug_toolbar',
)
INSTALLED_APPS_DJANGO_PLUGINS = (
    ### Django plugins
    'rest_framework',  #pip install djangorestframework, version 2.3.10
    'django_datatables_view',  #pip install django-datatables-view, version 1.6
    'djangojs',  #pip install django.js, version 0.8.1
)
INSTALLED_APPS_BIGPANDAMON_CORE = (
    ### BigPanDAmon core
    'core.common',
    'core.table',
    'core.pandajob',
    'core.schedresource',
    'core.htcondor',
    'core.datatables',
    'core.filebrowser',
    'core.pbm',
    'core.pbm.templatetags',
#    'core.graphic', #NOT-IMPLEMENTED
    'core.gspread',
    'django.contrib.staticfiles',
    'debug_toolbar',
)
COMMON_INSTALLED_APPS = \
    INSTALLED_APPS_DJANGO_FRAMEWORK + \
    INSTALLED_APPS_DJANGO_PLUGINS
INSTALLED_APPS = COMMON_INSTALLED_APPS + INSTALLED_APPS_BIGPANDAMON_CORE


### Django.js config
JS_I18N_APPS = ()
JS_I18N_APPS_EXCLUDE = INSTALLED_APPS_BIGPANDAMON_CORE

VERSIONS = {
    'core': core.__versionstr__,
}

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(dirname(core.__file__), 'templates'),
    join(dirname(common.__file__), 'templates'),
    join(dirname(core.filebrowser.__file__), 'templates'),
    join(dirname(core.pbm.__file__), 'templates'),
    join(dirname(core.pbm.__file__), 'templates'),

)



DEBUG_TOOLBAR_PATCH_SETTINGS = False
INTERNAL_IPS =('127.0.0.1', '192.168.0.1')
#DEBUG = True
DEBUG_TOOLBAR_CONFIG = {'INTERCEPT_REDIRECTS': False,}
DEBUG_TOOLBAR_PANELS = (
    'debug_toolbar.panels.versions.VersionsPanel',
    # Throwing AttributeError: 'module' object has no attribute 'getrusage'
    'debug_toolbar.panels.timer.TimerPanel',
    #'debug_toolbar.panels.settings_vars.SettingsVarsDebugPanel',
    'debug_toolbar.panels.headers.HeadersPanel',
    'debug_toolbar.panels.request.RequestPanel',
    'debug_toolbar.panels.templates.TemplatesPanel',
    'debug_toolbar.panels.sql.SQLPanel',
    'debug_toolbar.panels.signals.SignalsPanel',
    #'debug_toolbar.panels.logger.LoggingPanel',
)


INSTALLED_APPS_BIGPANDAMON_core = (
    ### BigPanDAmon core
    'core.common',
#    'core.table',
#    'core.graphics', #NOT-IMPLEMENTED
    'core.pandajob',
    'core.schedresource',
    'core.status_summary',
#    'core.htcondor', #NOT-NEEDED-IN-core
#    'core.task', #NOT-IMPLEMENTED
    'core.filebrowser',
    'core.pbm',
    'core.pbm.templatetags',
    'django_extensions',
)
INSTALLED_APPS = COMMON_INSTALLED_APPS + INSTALLED_APPS_BIGPANDAMON_core


ROOT_URLCONF = 'core.urls'

# email
EMAIL_SUBJECT_PREFIX = 'bigpandamon-core: '


