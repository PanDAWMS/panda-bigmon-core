"""
Collection of constants tuples for ART module
"""
__author__ = 'Tatiana Korchuganova'

from types import MappingProxyType

# vars
CACHE_TIMEOUT_MINUTES = 15
EOS_PREFIX = 'https://atlas-art-data.web.cern.ch/atlas-art-data/grid-output/'

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
    'other':  6
})

N_DAYS_DEFAULT = MappingProxyType({
    'test': 1,
    'updatejoblist': 30,
    'stability': 8,
    'errors': 1,
    'other':  6
})


# tuples
TEST_STATUS = (
    'finished',
    'failed',
    'active',
    'succeeded'
)


