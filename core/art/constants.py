"""
Collection of constants tuples for ART module
"""
__author__ = 'Tatiana Korchuganova'

from types import MappingProxyType

# vars
CACHE_TIMEOUT_MINUTES = 15


# dicts
DATETIME_FORMAT = MappingProxyType({
    'default': '%Y-%m-%d',
    'humanized': '%d %b %Y',
    'humanized_short': '%d %b',
})


# tuples
TEST_STATUS = (
    'finished',
    'failed',
    'active',
    'succeeded'
)


