"""
Collection of constants tuples
"""
__author__ = 'Tatiana Korchuganova'

from types import MappingProxyType


# dicts:
EVENT_SERVICE_JOB_TYPES = MappingProxyType({
    1: 'eventservice',
    2: 'esmerge',
    3: 'clone',
    4: 'jumbo',
    5: 'cojumbo',
})

# lists
JOB_STATES = (
    'pending',
    'defined',
    'waiting',
    'assigned',
    'throttled',
    'activated',
    'sent',
    'starting',
    'running',
    'holding',
    'transferring',
    'merging',
    'finished',
    'failed',
    'cancelled',
    'closed'
)

JOB_STATES_FINAL = (
    'finished',
    'failed',
    'cancelled',
    'closed',
    'merging'
)

RESOURCE_CAPABILITIES = (
    'SCORE',
    'MCORE',
    'SCORE_HIMEM',
    'MCORE_HIMEM'
)

EVENT_STATES = (
    'ready',
    'sent',
    'running',
    'finished',
    'cancelled',
    'discarded',
    'done',
    'failed',
    'fatal',
    'merged',
    'corrupted',
)
