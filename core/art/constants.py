"""
Collection of constants tuples for ART module
"""
__author__ = 'Tatiana Korchuganova'

from types import MappingProxyType

# vars
CACHE_TIMEOUT_MINUTES = 15
RETENTION_PERIOD_DAYS = 180
EOS_LINK_BASE = 'https://atlas-art-data.web.cern.ch/atlas-art-data/'
EOS_POSTFIX_GRID = 'grid-output/'
EOS_POSTFIX_LOCAL = 'local-output/'
INITIAL_LOCAL_ID = 1
TIMEOUT_THREAD_WAIT = 5
CACHE_KEY_GITLAB_PATH = 'art:gitlab_path_per_package_map'

# dicts
DATETIME_FORMAT = MappingProxyType({
    'default': '%Y-%m-%d',
    'humanized': '%d %b %Y',
    'humanized_short': '%d %b',
})

N_DAYS_MAX = MappingProxyType({
    'test': 1,
    'updatejoblist': 30,
    'stability': 15,
    'errors': 6,
    'main': 180,
    'other':  6
})

N_DAYS_DEFAULT = MappingProxyType({
    'test': 1,
    'updatejoblist': 30,
    'stability': 8,
    'errors': 1,
    'main': 30,
    'other':  6
})

TEST_STATUS_INDEX = MappingProxyType({
    'failed': 3,
    'active': 2,
    'finished': 1,
    'succeeded': 0,
})


# tuples
TEST_STATUS = (
    'finished',
    'failed',
    'active',
    'succeeded'
)

AUTHORIZED_HOSTS = ['atlas-art-local-01', 'atlas-art-local-02', 'aibuild16-025', 'aibuild16-026', 'aibuild-art-c4x2eph-001', 'aibuild-art-c4x2eph-002']
