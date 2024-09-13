import os
import core
import oracledb
oracledb.init_oracle_client(config_dir='/etc/tnsnames.ora')

try:
    from core.settings.local import DEBUG
except ImportError:
    DEBUG = False
try:
    from core.settings.local import SERVER_GATEWAY_INTERFACE
except ImportError:
    import os
    SERVER_GATEWAY_INTERFACE = os.environ.get('SERVER_GATEWAY_INTERFACE', 'WSGI')

try:
    from core.settings.local import ENABLE_DEBUG_TOOLBAR
except ImportError:
    ENABLE_DEBUG_TOOLBAR = False

ADMINS = (
    ('BigPanDA monitoring admins', os.environ.get('EMAIL_ADMINS', 'atlas-adc-pandamon-operation@cern.ch')),
)
MANAGERS = ADMINS

LANGUAGE_CODE = 'en-us'
LANGUAGE_NAME = 'English'
LANGUAGE_NAME_LOCAL = 'English'

TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

VERSION = core.__version__

STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
)

# List of callables that know how to import templates from various sources.

MIDDLEWARE = (
    'core.ddosprotection.DDOSMiddleware',
    'django.middleware.cache.UpdateCacheMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',  # for AJAX POST protection with csrf

    'django.contrib.auth.middleware.AuthenticationMiddleware',

    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.cache.FetchFromCacheMiddleware',
    'social_django.middleware.SocialAuthExceptionMiddleware',
    'core.oauth.CustomSocialAuthException.CustomSocialAuthExceptionMiddleware',

    'csp.middleware.CSPMiddleware',
)

ROOT_URLCONF = 'core.urls'

# Auth
AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
    'core.oauth.Cernauth2.CernAuthOIDC',
    'social_core.backends.google.GoogleOAuth2',
    'social_core.backends.github.GithubOAuth2',
    'core.oauth.indigoiam.IndigoIamOIDC',
)
AUTH_USER_MODEL = 'oauth.BPUser'

SOCIAL_AUTH_URL_NAMESPACE = 'social'
SOCIAL_AUTH_STRATEGY = 'social_django.strategy.DjangoStrategy'
SOCIAL_AUTH_STORAGE = 'social_django.models.DjangoStorage'
REDIRECT_STATE = False
LOGIN_URL = 'login'
#SOCIAL_AUTH_EXTRA_DATA = True

SOCIAL_AUTH_LOGIN_ERROR_URL = '/loginerror/'
LOGIN_REDIRECT_URL = '/'

# Google OAuth2 (google-oauth2)
SOCIAL_AUTH_GOOGLE_OAUTH2_IGNORE_DEFAULT_SCOPE = True
SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = [
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
]

SOCIAL_AUTH_PIPELINE = (
    'social_core.pipeline.social_auth.social_details',
    'social_core.pipeline.social_auth.social_uid',
    'social_core.pipeline.social_auth.social_user',
    'social_core.pipeline.user.get_username',
    'social_core.pipeline.user.create_user',
    'social_core.pipeline.social_auth.associate_user',
    'social_core.pipeline.social_auth.load_extra_data',
    'social_core.pipeline.user.user_details',
)


# installed apps
INSTALLED_APPS_DJANGO_FRAMEWORK = (
    # Django framework
    'channels',
    'social_django',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.humanize',
)

INSTALLED_APPS_DJANGO_PLUGINS = (
    # Django plugins
    'rest_framework',
    'django_datatables_view',
    'django_extensions'
)
COMMON_INSTALLED_APPS = \
    INSTALLED_APPS_DJANGO_FRAMEWORK + \
    INSTALLED_APPS_DJANGO_PLUGINS


INSTALLED_APPS_BIGPANDAMON_CORE = (
    # BigPanDAmon core
    'core.oauth',
    'core.common',
    'core.pandajob',
    'core.schedresource',
    'core.dashboards',
    'core.status_summary',
)

INSTALLED_APPS_EXTRA = [
    # "core.admin",
    "core.art",
    "core.buildmonitor",
    "core.compare",
    "core.datacarousel",
    "core.errorsscattering",
    "core.filebrowser",
    "core.globalshares",
    "core.grafana",
    "core.harvester",
    "core.iDDS",
    "core.mlflowdynamic",
    "core.reports",
    "core.runningprod",
    "core.panda_client",
    "core.kafka"
]

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer"
    }
}

if len(INSTALLED_APPS_EXTRA) > 0:
    INSTALLED_APPS_BIGPANDAMON_CORE += tuple([str(app_name) for app_name in INSTALLED_APPS_EXTRA])

# Django.js config
JS_I18N_APPS = ()
JS_I18N_APPS_EXCLUDE = INSTALLED_APPS_BIGPANDAMON_CORE

INSTALLED_APPS = COMMON_INSTALLED_APPS + INSTALLED_APPS_BIGPANDAMON_CORE

if SERVER_GATEWAY_INTERFACE == 'ASGI':
    INSTALLED_APPS = ('daphne',) + INSTALLED_APPS
    ASGI_APPLICATION = 'core.asgi.application'

if DEBUG and ENABLE_DEBUG_TOOLBAR:
    MIDDLEWARE += (
        'debug_toolbar.middleware.DebugToolbarMiddleware',
    )
    INSTALLED_APPS += (
        'debug_toolbar',
    )
    DEBUG_TOOLBAR_PATCH_SETTINGS = False
    INTERNAL_IPS = ('127.0.0.1', '192.168.0.1', )
    DEBUG_TOOLBAR_CONFIG = {
        'INTERCEPT_REDIRECTS': False,
        'SHOW_TOOLBAR_CALLBACK': lambda request: True,
    }
    DEBUG_TOOLBAR_PANELS = (
        'debug_toolbar.panels.versions.VersionsPanel',
        # Throwing AttributeError: 'module' object has no attribute 'getrusage'
        'debug_toolbar.panels.timer.TimerPanel',
        # 'debug_toolbar.panels.settings_vars.SettingsVarsDebugPanel',
        'debug_toolbar.panels.headers.HeadersPanel',
        'debug_toolbar.panels.request.RequestPanel',
        'debug_toolbar.panels.templates.TemplatesPanel',
        'debug_toolbar.panels.sql.SQLPanel',
        'debug_toolbar.panels.signals.SignalsPanel',
        # 'debug_toolbar.panels.logger.LoggingPanel',
    )


SESSION_SERIALIZER = "core.libs.CustomJSONSerializer.CustomJSONSerializer"
SESSION_ENGINE = "django.contrib.sessions.backends.cached_db"

# email
EMAIL_SUBJECT_PREFIX = '[BigPanDAmon]'
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

DKB_CAMPAIGN_URL = 'http://aiatlas172.cern.ch:5080/campaign/stat'
ML_FLOW_UPSTREAM = 'https://bigpanda-mlflow.web.cern.ch/'

DATA_CAROUSEL_MAIL_DELAY_DAYS = 10
DATA_CAROUSEL_MAIL_REPEAT = 1
