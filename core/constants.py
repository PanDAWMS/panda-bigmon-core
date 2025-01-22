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
    6: 'finegrained'
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
    'closed'
)

EVENT_STATES = MappingProxyType({
    0: 'ready',
    1: 'sent',
    2: 'running',
    3: 'finished',
    4: 'cancelled',
    5: 'discarded',
    6: 'done',
    7: 'failed',
    8: 'fatal',
    9: 'merged',
    10: 'corrupted',
})

TASK_STATES = (
    'registered',
    'defined',
    'assigning',
    'ready',
    'pending',
    'scouting',
    'scouted',
    'running',
    'prepared',
    'done',
    'failed',
    'finished',
    'aborting',
    'aborted',
    'finishing',
    'topreprocess',
    'preprocessing',
    'tobroken',
    'broken',
    'toretry',
    'toincexec',
    'rerefine'
)

TASK_STATES_FINAL = (
    'broken',
    'aborted',
    'done',
    'finished',
    'failed'
)

JOB_FIELDS_ERROR_VIEW = (
    'computingelement',
    'computingsite',
    'eventservice',
    'jeditaskid',
    'jobstatus',
    'processingtype',
    'prodsourcelabel',
    'produsername',
    'specialhandling',
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

JOB_FIELDS_ATTR_SUMMARY = (
    'processingtype',
    'computingsite',
    'jobstatus',
    'prodsourcelabel',
    'produsername',
    'jeditaskid',
    'workinggroup',
    'transformation',
    'cloud',
    'homepackage',
    'inputfileproject',
    'inputfiletype',
    'outputfiletype',
    'attemptnr',
    'specialhandling',
    'priorityrange',
    'reqid',
    'minramcount',
    'eventservice',
    'jobsubstatus',
    'nucleus',
    'gshare',
    'resourcetype'
)

SITE_FIELDS_STANDARD = (
    'cloud',
    'gocname',
    'status',
    'tier',
    'type',
    'cloud',
    'country',
    'harvester',
    'copytool',
    'system',
    'workflow',
)

TASK_FIELDS_STANDARD = (
    'workqueue_id',
    'tasktype',
    'superstatus',
    'status',
    'corecount',
    'taskpriority',
    'currentpriority',
    'username',
    'transuses',
    'transpath',
    'workinggroup',
    'processingtype',
    'cloud',
    'campaign',
    'project',
    'stream',
    'tag',
    'reqid',
    'ramcount',
    'nucleus',
    'eventservice',
    'gshare',
    'container_name',
    'attemptnr',
    'site'
)

JOB_FIELDS = (
    'corecount',
    'jobsubstatus',
    'produsername',
    'cloud',
    'computingsite',
    'cpuconsumptiontime',
    'jobstatus',
    'transformation',
    'prodsourcelabel',
    'specialhandling',
    'vo',
    'modificationtime',
    'pandaid',
    'atlasrelease',
    'jobsetid',
    'processingtype',
    'workinggroup',
    'jeditaskid',
    'taskid',
    'currentpriority',
    'creationtime',
    'starttime',
    'endtime',
    'brokerageerrorcode',
    'brokerageerrordiag',
    'ddmerrorcode',
    'ddmerrordiag',
    'exeerrorcode',
    'exeerrordiag',
    'jobdispatchererrorcode',
    'jobdispatchererrordiag',
    'piloterrorcode',
    'piloterrordiag',
    'superrorcode',
    'superrordiag',
    'taskbuffererrorcode',
    'taskbuffererrordiag',
    'transexitcode',
    'destinationse',
    'homepackage',
    'inputfileproject',
    'inputfiletype',
    'attemptnr',
    'jobname',
    'computingelement',
    'proddblock',
    'destinationdblock',
    'reqid',
    'minramcount',
    'statechangetime',
    'avgvmem',
    'maxvmem',
    'maxpss',
    'maxrss',
    'nucleus',
    'eventservice',
    'nevents',
    'gshare',
    'noutputdatafiles',
    'parentid',
    'actualcorecount',
    'schedulerid',
    'container_name',
    'maxattempt',
    'pilotid',
    'jobmetrics',
    'resourcetype',
    'commandtopilot',
    'cmtconfig'
)

TIME_LIMIT_OPTIONS = (
    'days',
    'hours',
    'date_from',
    'date_to',
    'earlierthan',
    'earlierthandays',
)