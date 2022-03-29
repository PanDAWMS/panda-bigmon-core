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

JOB_STATES_SITE = (
    'defined',
    'waiting',
    'assigned',
    'throttled',
    'activated',
    'sent',
    'starting',
    'running',
    'holding',
    'merging',
    'transferring',
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

TASK_STATES_FINAL = (
    'broken',
    'aborted',
    'done',
    'finished',
    'failed'
)

JOB_FIELDS_ERROR_VIEW = (
    'cloud',
    'computingelement',
    'computingsite',
    'eventservice',
    'jeditaskid',
    'jobstatus',
    'processingtype',
    'prodsourcelabel',
    'produsername',
    'specialhandling',
    'taskid',
    'transformation',
    'reqid',
    'workinggroup',
)


JOB_ERROR_CATEGORIES = (
    {'name': 'brokerage', 'error': 'brokerageerrorcode', 'diag': 'brokerageerrordiag', 'title': 'Brokerage error'},
    {'name': 'ddm', 'error': 'ddmerrorcode', 'diag': 'ddmerrordiag', 'title': 'DDM error'},
    {'name': 'exe', 'error': 'exeerrorcode', 'diag': 'exeerrordiag', 'title': 'Executable error'},
    {'name': 'jobdispatcher', 'error': 'jobdispatchererrorcode', 'diag': 'jobdispatchererrordiag',
        'title': 'Dispatcher error'},
    {'name': 'pilot', 'error': 'piloterrorcode', 'diag': 'piloterrordiag', 'title': 'Pilot error'},
    {'name': 'sup', 'error': 'superrorcode', 'diag': 'superrordiag', 'title': 'Sup error'},
    {'name': 'taskbuffer', 'error': 'taskbuffererrorcode', 'diag': 'taskbuffererrordiag', 'title': 'Task buffer error'},
    {'name': 'transformation', 'error': 'transexitcode', 'diag': None, 'title': 'Trf exit code'},
)


