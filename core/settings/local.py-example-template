# Database settings
# Make this unique, and don't share it with anybody.

import os,sys
try:
    execfile(os.environ['LOCAL_EXTRA_SETTINGS'])
except:
    print "Error with LOCAL_EXTRA_SETTINGS"
    sys.exit(1)

# set default datetime format for datetime.datetime.strftime()
defaultDatetimeFormatMySQL = "%Y-%m-%d %H:%M:%SZ"
defaultDatetimeFormatOracle = "%Y-%m-%d %H:%M:%S"
defaultDatetimeFormat = defaultDatetimeFormatOracle


FILEBROWSER_DIRECTORY = "filebrowser"
#X509_USER_PROXY = "/data/atlpan/x509up_u25606"
X509_USER_PROXY = "/tmp/x509up_u25606"
CAPATH = "/etc/grid-security/certificates"
RUCIO_REDIRECT_HOST = "https://rucio-lb-prod.cern.ch"
RUCIO_AUTH_HOST = "https://rucio-auth-prod.cern.ch"
RUCIO_SERVER_HOST = "https://rucio-lb-prod.cern.ch"
MEDIA_URL = '/media/'


# log directory
#LOG_ROOT = "/data/bigpandamon_virtualhosts/core/logs"
#LOG_ROOT = "/data/wenaus/bigpandamon_virtualhosts/twrpm/logs"
LOG_ROOT = "/home/podolsky/pandamon/log"
#DEBUG = False
#USE_TZ = True
USE_TZ = False
