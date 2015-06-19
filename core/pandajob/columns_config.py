"""
    pandajob.columns_config

"""
import logging
_logger = logging.getLogger('bigpandamon')

COLUMNS = {}
ORDER_COLUMNS = {}
COL_TITLES = {}
FILTERS = {}
UPDATE_COL_TITLES = {}
SUMMARY_FIELDS = {}
SMRYCOL_TITLES = {}
DEFAULT_COLDEF = {'sort': True, 'vis': True, 'c': '', 't': ''}

def skimColumns(myColumnsID, allColumnsID):
    """
        skimColumns(myColumnsID, allColumnsID): 
        get intersection of columns listed in COLUMNS[myColumnsID]
            and             columns listed in COLUMNS[allColumnsID]
    
    """
    cols = []
    try:
        cols = COLUMNS[myColumnsID]
    except:
        return cols

    try:
        COLUMNS[allColumnsID]
    except:
        return cols

    cols = filter(lambda itm:itm in COLUMNS[myColumnsID], COLUMNS[allColumnsID])

    return cols


def getTitles(myColumnsID, allColumnsID, smry=0):
    """
        getTitles(myColumnsID, allColumnsID)
        get column titles for columns in COLUMNS[myColumnsID]
        
    """
    titles = []
    try:
#        myCols = ORDER_COLUMNS[myColumnsID]
#        titles = [ x for colname in myCols for x in COL_TITLES[allColumnsID] if x['c'] == colname ]
        columns = ORDER_COLUMNS[myColumnsID]
        if smry:
            columns += SUMMARY_FIELDS[myColumnsID]

        for colname in columns:
            colDef = {}
            try:
                colDef = [x for x in COL_TITLES[allColumnsID] \
                          if x['c'] == colname ][0]
                if colname in UPDATE_COL_TITLES[myColumnsID].keys():
                    colDef.update(UPDATE_COL_TITLES[myColumnsID][colname])
            except:
                colDef = colDef.update(DEFAULT_COLDEF)
                colDef['c'] = colname
                colDef['t'] = colname
            titles.append(colDef)
    except:
        _logger.error('something went wrong: titles=' + str(titles))
        return titles
    _logger.debug('returning: titles=' + str(titles))
    return titles


def getTitlesSmry(myColumnsID, allColumnsID, smry=1):
    return getTitles(myColumnsID, allColumnsID, smry)


### PanDAjob - all
COLUMNS['PanDAjob-all'] = [\
        'pandaid', 'jobdefinitionid', 'schedulerid', 'pilotid', 'creationtime', \
        'creationhost', 'modificationtime', 'modificationhost', 'atlasrelease', \
        'transformation', 'homepackage', 'prodserieslabel', 'prodsourcelabel', \
        'produserid', 'assignedpriority', 'currentpriority', 'attemptnr', \
        'maxattempt', 'jobstatus', 'jobname', 'maxcpucount', 'maxcpuunit', \
        'maxdiskcount', 'maxdiskunit', 'ipconnectivity', 'minramcount', \
        'minramunit', 'starttime', 'endtime', 'cpuconsumptiontime', \
        'cpuconsumptionunit', 'commandtopilot', 'transexitcode', \
        'piloterrorcode', 'piloterrordiag', 'exeerrorcode', 'exeerrordiag', \
        'superrorcode', 'superrordiag', 'ddmerrorcode', 'ddmerrordiag', \
        'brokerageerrorcode', 'brokerageerrordiag', 'jobdispatchererrorcode', \
        'jobdispatchererrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', \
        'computingsite', 'computingelement', 'jobparameters', 'metadata', \
        'proddblock', 'dispatchdblock', 'destinationdblock', 'destinationse', \
        'nevents', 'grid', 'cloud', 'cpuconversion', 'sourcesite', \
        'destinationsite', 'transfertype', 'taskid', 'cmtconfig', \
        'statechangetime', 'proddbupdatetime', 'lockedby', 'relocationflag', \
        'jobexecutionid', 'vo', 'pilottiming', 'workinggroup', 'processingtype', \
        'produsername', 'ninputfiles', 'countrygroup', 'batchid', 'parentid', \
        'specialhandling', 'jobsetid', 'corecount', 'ninputdatafiles', \
        'inputfiletype', 'inputfileproject', 'inputfilebytes', \
        'noutputdatafiles', 'outputfilebytes', 'jobmetrics', 'workqueue_id', \
        'jeditaskid', 'jobsubstatus', 'actualcorecount' \
]
ORDER_COLUMNS['PanDAjob-all'] = [\
            'pandaid', 'jobdefinitionid', 'creationtime', 'modificationtime', \
            'jobstatus', 'currentpriority', 'cloud', \
#            'produserid', 'destinationsite'\
]
COL_TITLES['PanDAjob-all'] = [ \
    {'sort': True, 'vis': True, 'c': 'pandaid', 't': 'PanDA ID'}, \
    {'sort': True, 'vis': True, 'c': 'jobdefinitionid', 't': 'Job Definition ID'}, \
    {'sort': True, 'vis': True, 'c': 'creationtime', 't': 'Created'}, \
    {'sort': True, 'vis': True, 'c': 'modificationtime', 't': 'Modified'}, \
    {'sort': True, 'vis': True, 'c': 'jobstatus', 't': 'Job Status'}, \
    {'sort': True, 'vis': True, 'c': 'currentpriority', 't': 'Current Priority'}, \
    {'sort': True, 'vis': True, 'c': 'cloud', 't': 'Cloud'}, \
    {'sort': True, 'vis': False, 'c': 'schedulerid', 't': 'Scheduler ID'}, \
    {'sort': True, 'vis': False, 'c': 'pilotid', 't': 'Pilot ID'}, \
    {'sort': True, 'vis': False, 'c': 'creationhost', 't': 'Creation Host'}, \
    {'sort': True, 'vis': False, 'c': 'modificationhost', 't': 'Modification Host'}, \
    {'sort': True, 'vis': False, 'c': 'atlasrelease', 't': 'ATLAS release'}, \
    {'sort': True, 'vis': False, 'c': 'transformation', 't': 'Transformation'}, \
    {'sort': True, 'vis': False, 'c': 'homepackage', 't': 'Home package'}, \
    {'sort': True, 'vis': False, 'c': 'prodserieslabel', 't': 'prodserieslabel'}, \
    {'sort': True, 'vis': False, 'c': 'prodsourcelabel', 't': 'prodsourcelabel'}, \
    {'sort': True, 'vis': False, 'c': 'produserid', 't': 'Prod User ID'}, \
    {'sort': True, 'vis': False, 'c': 'assignedpriority', 't': 'Assigned Priority'}, \
    {'sort': True, 'vis': False, 'c': 'attemptnr', 't': 'Attempt #'}, \
    {'sort': True, 'vis': False, 'c': 'maxattempt', 't': 'Max Attempt'}, \
    {'sort': True, 'vis': False, 'c': 'jobname', 't': 'Job name'}, \
    {'sort': True, 'vis': False, 'c': 'maxcpucount', 't': 'Max CPU count'}, \
    {'sort': True, 'vis': False, 'c': 'maxcpuunit', 't': 'Unit of Max CPU count'}, \
    {'sort': True, 'vis': False, 'c': 'maxdiskcount', 't': 'Max Disk count'}, \
    {'sort': True, 'vis': False, 'c': 'maxdiskunit', 't': 'Unit of Max Disk count'}, \
    {'sort': True, 'vis': False, 'c': 'ipconnectivity', 't': 'IP Connectivity'}, \
    {'sort': True, 'vis': False, 'c': 'minramcount', 't': 'Min RAM count'}, \
    {'sort': True, 'vis': False, 'c': 'minramunit', 't': 'Unit of Min RAM count'}, \
    {'sort': True, 'vis': True, 'c': 'starttime', 't': 'Started'}, \
    {'sort': True, 'vis': True, 'c': 'endtime', 't': 'Ended'}, \
    {'sort': True, 'vis': False, 'c': 'cpuconsumptiontime', 't': 'CPU Consumption time'}, \
    {'sort': True, 'vis': False, 'c': 'cpuconsumptionunit', 't': 'Unit of CPU Consumption time'}, \
    {'sort': True, 'vis': False, 'c': 'commandtopilot', 't': 'Command to pilot'}, \
    {'sort': True, 'vis': False, 'c': 'transexitcode', 't': 'Trans Exit Code'}, \
    {'sort': True, 'vis': False, 'c': 'piloterrorcode', 't': 'Pilot Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'piloterrordiag', 't': 'Pilot Error Diag'}, \
    {'sort': True, 'vis': False, 'c': 'exeerrorcode', 't': 'Exe Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'exeerrordiag', 't': 'Exe Error Diag'}, \
    {'sort': True, 'vis': False, 'c': 'superrorcode', 't': 'Sup Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'superrordiag', 't': 'Sup Error Diag'}, \
    {'sort': True, 'vis': False, 'c': 'ddmerrorcode', 't': 'DDM Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'ddmerrordiag', 't': 'DDM Error Diag'}, \
    {'sort': True, 'vis': False, 'c': 'brokerageerrorcode', 't': 'Brokerage Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'brokerageerrordiag', 't': 'Brokerage Error Diag'}, \
    {'sort': True, 'vis': False, 'c': 'jobdispatchererrorcode', 't': 'Job Dispatcher Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'jobdispatchererrordiag', 't': 'Job Dispatcher Error Diag'}, \
    {'sort': True, 'vis': False, 'c': 'taskbuffererrorcode', 't': 'Taskbuffer Error Code'}, \
    {'sort': True, 'vis': False, 'c': 'taskbuffererrordiag', 't': 'Taskbuffer Error Diag'}, \
    {'sort': True, 'vis': True, 'c': 'computingsite', 't': 'Computing Site'}, \
    {'sort': True, 'vis': False, 'c': 'computingelement', 't': 'Computing Element'}, \
    {'sort': True, 'vis': False, 'c': 'jobparameters', 't': 'Job Parameters'}, \
    {'sort': True, 'vis': False, 'c': 'metadata', 't': 'Metadata'}, \
    {'sort': True, 'vis': False, 'c': 'proddblock', 't': 'proddblock'}, \
    {'sort': True, 'vis': False, 'c': 'dispatchdblock', 't': 'dispatchdblock'}, \
    {'sort': True, 'vis': False, 'c': 'destinationdblock', 't': 'destinationdblock'}, \
    {'sort': True, 'vis': False, 'c': 'destinationse', 't': 'destinationse'}, \
    {'sort': True, 'vis': False, 'c': 'nevents', 't': 'N events'}, \
    {'sort': True, 'vis': False, 'c': 'grid', 't': 'Grid'}, \
    {'sort': True, 'vis': False, 'c': 'cpuconversion', 't': 'CPU Conversion'}, \
    {'sort': True, 'vis': False, 'c': 'sourcesite', 't': 'Source Site'}, \
    {'sort': True, 'vis': False, 'c': 'destinationsite', 't': 'Destination Site'}, \
    {'sort': True, 'vis': False, 'c': 'transfertype', 't': 'Transfer Type'}, \
    {'sort': True, 'vis': False, 'c': 'taskid', 't': 'Task ID'}, \
    {'sort': True, 'vis': False, 'c': 'cmtconfig', 't': 'CMT config'}, \
    {'sort': True, 'vis': False, 'c': 'statechangetime', 't': 'State Change Time'}, \
    {'sort': True, 'vis': False, 'c': 'proddbupdatetime', 't': 'Prod DB Update Time'}, \
    {'sort': True, 'vis': False, 'c': 'lockedby', 't': 'Locked By'}, \
    {'sort': True, 'vis': False, 'c': 'relocationflag', 't': 'Relocation Flag'}, \
    {'sort': True, 'vis': False, 'c': 'jobexecutionid', 't': 'Job Execution ID'}, \
    {'sort': True, 'vis': False, 'c': 'vo', 't': 'VO'}, \
    {'sort': True, 'vis': False, 'c': 'pilottiming', 't': 'Pilot timing'}, \
    {'sort': True, 'vis': True, 'c': 'workinggroup', 't': 'Working Group'}, \
    {'sort': True, 'vis': False, 'c': 'processingtype', 't': 'Processing Type'}, \
    {'sort': True, 'vis': True, 'c': 'produsername', 't': 'Owner'}, \
    {'sort': True, 'vis': False, 'c': 'ninputfiles', 't': 'N input files'}, \
    {'sort': True, 'vis': False, 'c': 'countrygroup', 't': 'Country Group'}, \
    {'sort': True, 'vis': False, 'c': 'batchid', 't': 'Batch ID'}, \
    {'sort': True, 'vis': False, 'c': 'parentid', 't': 'Parent ID'}, \
    {'sort': True, 'vis': False, 'c': 'specialhandling', 't': 'Special Handling'}, \
    {'sort': True, 'vis': False, 'c': 'jobsetid', 't': 'Jobset ID'}, \
    {'sort': True, 'vis': False, 'c': 'corecount', 't': 'Core Count'}, \
    {'sort': True, 'vis': False, 'c': 'ninputdatafiles', 't': 'N input data files'}, \
    {'sort': True, 'vis': False, 'c': 'inputfiletype', 't': 'Input file type'}, \
    {'sort': True, 'vis': False, 'c': 'inputfileproject', 't': 'Input file project'}, \
    {'sort': True, 'vis': False, 'c': 'inputfilebytes', 't': 'Input file bytes'}, \
    {'sort': True, 'vis': False, 'c': 'noutputdatafiles', 't': 'N output data files'}, \
    {'sort': True, 'vis': False, 'c': 'outputfilebytes', 't': 'Output file bytes'}, \
    {'sort': True, 'vis': False, 'c': 'jobmetrics', 't': 'Job Metrics'}, \
    {'sort': True, 'vis': False, 'c': 'workqueue_id', 't': 'Work queue ID'}, \
    {'sort': True, 'vis': True, 'c': 'jeditaskid', 't': 'Task ID'}, \
    {'sort': True, 'vis': True, 'c': 'jobsubstatus', 't': 'Job Substatus'}, \
    {'sort': True, 'vis': True, 'c': 'actualcorecount', 't': 'Actual Core Count'}, \
]
SMRYCOL_TITLES['PanDAjob-all'] = {}
UPDATE_COL_TITLES['PanDAjob-all'] = {}
FILTERS['PanDAjob-all'] = [ \
    { 'name': 'ProdUserName', 'field': 'produsername', 'filterField': 'produsername', 'type': 'string' }, \
    { 'name': 'JediTaskID', 'field': 'jeditaskid', 'filterField': 'jeditaskid', 'type': 'integer' }, \
    { 'name': 'JobStatus', 'field': 'jobstatus', 'filterField': 'jobstatus', 'type': 'stringMultiple' }, \
    { 'name': 'WG', 'field': 'workinggroup', 'filterField': 'workinggroup', 'type': 'string' }, \
    { 'name': 'Cloud', 'field': 'cloud', 'filterField': 'cloud', 'type': 'stringMultiple' }, \
    { 'name': 'Site', 'field': 'computingsite', 'filterField': 'computingsite', 'type': 'string' }, \
    { 'name': 'ProdSourceLabel', 'field': 'prodsourcelabel', 'filterField': 'prodsourcelabel', 'type': 'string' }, \
    { 'name': 'CreationFrom', 'field': 'creationtime', 'filterField': 'creationtime__gte', 'type': 'datetime' }, \
    { 'name': 'CreationTo', 'field': 'creationtime', 'filterField': 'creationtime__lte', 'type': 'datetime'}, \
    { 'name': 'ModificationFrom', 'field': 'modificationtime', 'filterField': 'modificationtime__gte', 'type': 'datetime' }, \
    { 'name': 'ModificationTo', 'field': 'modificationtime', 'filterField': 'modificationtime__lte', 'type': 'datetime'}, \
    { 'name': 'Trf', 'field': 'transformation', 'filterField': 'transformation', 'type': 'string'}, \
    { 'name': 'Rls', 'field': 'atlasrelease', 'filterField': 'atlasrelease', 'type': 'string'}, \
    { 'name': 'ProcessingType', 'field': 'processingtype', 'filterField': 'processingtype', 'type': 'string'}, \
    { 'name': 'JobsetID', 'field': 'jobsetid', 'filterField': 'jobsetid', 'type': 'integer'}, \
]
SUMMARY_FIELDS['PanDAjob-all'] = [
        'jobstatus', \
        'produsername', \
        'atlasrelease', \
        'prodsourcelabel', \
        'processingtype', \
        'transformation',
        'workinggroup', \
        'computingsite', \
        'cloud', \
        'jeditaskid' \
]

### reverse URL: 'api-datatables-jedi-jobs-in-task'
COLUMNS['api-datatables-jedi-jobs-in-task'] = [\
        'pandaid', 'jeditaskid', 'produsername', 'workinggroup', \
        'creationtime', 'modificationtime', 'starttime', 'endtime', \
        'jobstatus', 'currentpriority', 'computingsite', 'cloud', \
        'jobsetid', 'prodsourcelabel'
    ]
ORDER_COLUMNS['api-datatables-jedi-jobs-in-task'] = [\
        'jeditaskid', 'pandaid', \
        'jobstatus', \
        'creationtime', 'modificationtime', 'starttime', 'endtime', \
        'cloud', 'computingsite', 'currentpriority', \
        'produsername', 'workinggroup', \
        'jobsetid'
    ]
UPDATE_COL_TITLES['api-datatables-jedi-jobs-in-task'] = { \
    'workinggroup': {'vis': False, 'sort': False}, \
    'cloud': {'vis': False, 'sort': False}, \
#    'prodsourcelabel': {'vis': False, 'sort': False}, \
    'jobsetid': {'vis': False, 'sort': False}, \
    'produsername': {'t': 'Owner'}, \
}
COL_TITLES['api-datatables-jedi-jobs-in-task'] = \
    getTitles('api-datatables-jedi-jobs-in-task', 'PanDAjob-all')
#COL_TITLES['api-datatables-jedi-jobs-in-task'] = \
#    updateTitles('api-datatables-jedi-jobs-in-task', \
#                 COL_TITLES['api-datatables-jedi-jobs-in-task'], \
#                 updateColTitlesData\
#    )
FILTERS['api-datatables-jedi-jobs-in-task'] = FILTERS['PanDAjob-all']
SUMMARY_FIELDS['api-datatables-jedi-jobs-in-task'] = SUMMARY_FIELDS['PanDAjob-all']
SMRYCOL_TITLES['api-datatables-jedi-jobs-in-task'] = \
    getTitlesSmry('api-datatables-jedi-jobs-in-task', 'PanDAjob-all', smry=True)



### reverse URL: 'ActiveUsers-all'
COLUMNS['ActiveUsers-all'] = [\
        'name'
    ]
ORDER_COLUMNS['ActiveUsers-all'] = [\
        'name'
    ]
UPDATE_COL_TITLES['ActiveUsers-all'] = {}
COL_TITLES['ActiveUsers-all'] = [
    {'sort': True, 'vis': True, 'c': 'name', 't': 'Active PanDA User'}
]
FILTERS['ActiveUsers-all'] = []
SUMMARY_FIELDS['ActiveUsers-all'] = []
SMRYCOL_TITLES['ActiveUsers-all'] = []


### reverse URL: 'api-datatables-user-list-active-users'
COLUMNS['api-datatables-user-list-active-users'] = COLUMNS['ActiveUsers-all']
ORDER_COLUMNS['api-datatables-user-list-active-users'] = ORDER_COLUMNS['ActiveUsers-all']
UPDATE_COL_TITLES['api-datatables-user-list-active-users'] = UPDATE_COL_TITLES['ActiveUsers-all']
COL_TITLES['api-datatables-user-list-active-users'] = COL_TITLES['ActiveUsers-all']
FILTERS['api-datatables-user-list-active-users'] = FILTERS['ActiveUsers-all']
SUMMARY_FIELDS['api-datatables-user-list-active-users'] = SUMMARY_FIELDS['ActiveUsers-all']
SMRYCOL_TITLES['api-datatables-user-list-active-users'] = SMRYCOL_TITLES['ActiveUsers-all']


### reverse URL: 'api-datatables-user-list-user-activity'
COLUMNS['api-datatables-user-list-user-activity'] = [
        'pandaid', 'jobsetid', 'produsername', 'workinggroup', \
        'creationtime', 'modificationtime', 'starttime', 'endtime', \
        'jobstatus', 'currentpriority', 'computingsite', 'cloud', \
        'prodsourcelabel', 'jeditaskid', 'attemptnr'
]
ORDER_COLUMNS['api-datatables-user-list-user-activity'] = [
        'jobsetid', 'pandaid', \
        'jobstatus', \
        'creationtime', 'modificationtime', 'starttime', 'endtime', \
        'cloud', 'computingsite', 'currentpriority', \
        'produsername', 'workinggroup', 'jeditaskid', 'attemptnr' \
]
UPDATE_COL_TITLES['api-datatables-user-list-user-activity'] = { \
    'workinggroup': {'vis': False, 'sort': False}, \
    'cloud': {'vis': False, 'sort': False}, \
#    'prodsourcelabel': {'vis': False, 'sort': False}, \
    'jobsetid': {'vis': False, 'sort': False}, \
#    'produsername': {'t': 'Owner'}, \
    'attemptnr': {'vis': True, 'sort': False}, \
}

COL_TITLES['api-datatables-user-list-user-activity'] = \
    getTitles('api-datatables-user-list-user-activity', 'PanDAjob-all')
FILTERS['api-datatables-user-list-user-activity'] = FILTERS['api-datatables-jedi-jobs-in-task']
SUMMARY_FIELDS['api-datatables-user-list-user-activity'] = [\
        'jobsetid', \
        'jobstatus', \
        'produsername', \
        'atlasrelease', \
        'prodsourcelabel', \
        'processingtype', \
        'transformation',
        'workinggroup', \
        'computingsite', \
        'cloud', \
        'jeditaskid' \
]
SMRYCOL_TITLES['api-datatables-user-list-user-activity'] = \
    getTitlesSmry('api-datatables-user-list-user-activity', 'PanDAjob-all', smry=True)





### reverse URL: 'api-reprocessing-jobs-in-task-smry'
COLUMNS['api-reprocessing-jobs-in-task-smry'] = [ \
        'pandaid', 'jobstatus', 'jobname', 'attemptnr', 'workinggroup', \
        'jeditaskid', 'modificationtime' \
]
ORDER_COLUMNS['api-reprocessing-jobs-in-task-smry'] = [ \
        'pandaid', 'jobstatus', 'jobname', 'attemptnr' \
]
UPDATE_COL_TITLES['api-reprocessing-jobs-in-task-smry'] = { \
    'pandaid': {'vis': True, 'sort': False}, \
    'jobstatus': {'vis': True, 'sort': False}, \
    'jobname': {'vis': True, 'sort': False}, \
    'attemptnr': {'vis': True, 'sort': False}, \
    'jobsetid': {'vis': False, 'sort': False}, \
}
COL_TITLES['api-reprocessing-jobs-in-task-smry'] = \
    getTitles('api-reprocessing-jobs-in-task-smry', 'PanDAjob-all')
FILTERS['api-reprocessing-jobs-in-task-smry'] = [\
#    { 'name': 'taskname', 'field': 'taskname', 'filterField': 'taskname', 'type': 'string' }, \
    { 'name': 'modificationtime_from', 'field': 'modificationtime', 'filterField': 'modificationtime__gte', 'type': 'datetime' }, \
    { 'name': 'modificationtime_to', 'field': 'modificationtime', 'filterField': 'modificationtime__lte', 'type': 'datetime'}, \
    { 'name': 'workinggroup', 'field': 'workinggroup', 'filterField': 'workinggroup', 'type': 'string' }, \
    { 'name': 'jobstatus', 'field': 'jobstatus', 'filterField': 'jobstatus', 'type': 'stringMultiple' }, \
]
SUMMARY_FIELDS['api-reprocessing-jobs-in-task-smry'] = [\
        'pandaid', 'jobstatus', 'jobname', 'attemptnr', 'workinggroup', 'jeditaskid' \
]
SMRYCOL_TITLES['api-reprocessing-jobs-in-task-smry'] = \
    getTitlesSmry('api-reprocessing-jobs-in-task-smry', 'PanDAjob-all', smry=True)


