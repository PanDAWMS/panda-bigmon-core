from core.common.models import JediDatasets, JediDatasetContents, Filestable4, FilestableArch, TemporaryData
from core.pandajob.models import Jobsdefined4, Jobswaiting4, Jobsactive4, Jobsarchived4, Jobsarchived
import math, random, datetime, copy, logging
from django.db import connection
from dateutil.parser import parse
from datetime import datetime, timezone, timedelta
from core.settings.local import dbaccess, defaultDatetimeFormat
_logger_error = logging.getLogger('bigpandamon-error')

def fileList(jobs):
    newjobs = []
    if (len(jobs)>0):
        pandaIDQ = []
        query = {}
        datasetsFromFileTable = []
        JediDatasetContentsTable=[]
        JediDatasetsTable=[]
        datasetIDQ =set()
        for job in jobs:
            pandaIDQ.append(job['pandaid'])
        query['pandaid__in'] = pandaIDQ
        #query['type'] = 'INPUT'
        pandaLFN = {}
        datasetsFromFileTable.extend(
            Filestable4.objects.filter(**query).extra(where=["TYPE like %s"], params=["input"]).values())
        if len(datasetsFromFileTable) == 0:
            datasetsFromFileTable.extend(
                FilestableArch.objects.filter(**query).extra(where=["TYPE like %s"], params=["input"]).values())
        for dataset in datasetsFromFileTable:
            datasetIDQ.add(dataset['datasetid'])
            pandaLFN.setdefault(dataset['pandaid'],[]).append(dataset['lfn'])

        query['datasetid__in'] = list(datasetIDQ)
        JediDatasetContentsTable.extend(JediDatasetContents.objects.filter(**query).extra(where=["datasetid in (SELECT datasetid from ATLAS_PANDA.JEDI_DATASETS where masterid is NULL)"]).values())
        njob = {}

        for jds in JediDatasetContentsTable:
            if jds['pandaid'] not in njob:
                njob[jds['pandaid']] = {}
                if jds['nevents'] == None:
                    njob[jds['pandaid']]['nevents'] = 0
                else:
                    if jds['endevent']!=None and jds['startevent']!=None:
                        njob[jds['pandaid']]['nevents'] = int(jds['endevent'])+1-int(jds['startevent'])
                    else: njob[jds['pandaid']]['nevents'] = int(jds['nevents'])
                if jds['type']=='input':
                    njob[jds['pandaid']]['ninputs'] = []
                    njob[jds['pandaid']]['ninputs'].append(jds['lfn'])
            else:
                if jds['endevent'] != None and jds['startevent'] != None:
                    njob[jds['pandaid']]['nevents'] += int(jds['endevent']) + 1 - int(jds['startevent'])
                else:
                    if jds['nevents'] != None:
                        njob[jds['pandaid']]['nevents'] += int(jds['nevents'])
                    else: njob[jds['pandaid']]['nevents'] += 0
                if jds['type']=='input':
                    njob[jds['pandaid']]['ninputs'].append(jds['lfn'])
        listpandaidsDS = njob.keys()
        listpandaidsF4 = pandaLFN.keys()
        for job in jobs:
            if job['pandaid'] in listpandaidsDS:
                # job['nevents'] = int(math.fabs(job['nevents']-njob[job['pandaid']]['nevents']))
                job['nevents'] = njob[job['pandaid']]['nevents']
                if 'ninputs' in njob[job['pandaid']]:
                    job['ninputs'] = len(njob[job['pandaid']]['ninputs'])
                else:
                    job['ninputs'] = 0 #0
                    if job['processingtype'] == 'pmerge':
                        if len(job['jobinfo']) == 0:
                            job['jobinfo'] = 'Pmerge job'
                        else:
                            job['jobinfo'] += 'Pmerge job'
            else:
                #if isEventService(job):
                if job['pandaid'] in listpandaidsF4:
                    job['ninputs'] = len(pandaLFN[job['pandaid']])
                else: job['ninputs'] = 0
            if job['pandaid'] in listpandaidsF4 and 'ninputs' not in job:
                 job['ninputs'] = len(pandaLFN[job['pandaid']])
            #print str(job['pandaid'])+' '+str(job['ninputs'])
            #if job['ninputs']==0:
            #    job['ninputs'] = len(pandaLFN[job['pandaid']])
                #else: job['ninputs'] = len(pandaLFN[job['pandaid']])
            # if (job['jobstatus'] == 'finished' and job['nevents'] == 0) or (job['jobstatus'] == 'cancelled' and job['nevents'] == 0):
            #     job['ninputs'] = 0
            # if job['jobstatus'] == 'finished' and job['pandaid'] not in listpandaidsDS:
            #     job['ninputs'] = 0
            #     job['nevents'] = 0
            if job['nevents']!= 0 and 'ninputs' not in job and job['pandaid'] in listpandaidsF4:
                job['ninputs'] = len(pandaLFN[job['pandaid']])

    newjobs = jobs
    return newjobs


def insert_to_temp_table(list_of_items, transaction_key = -1):
    """Inserting to temp table
    :param list_of_items
    :return transactionKey and timestamp of instering
    """

    # if dbaccess['default']['ENGINE'].find('oracle') >= 0:
    #     tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    # else:
    #     tmpTableName = "TMP_IDS1"

    if transaction_key == -1:
        random.seed()
        transaction_key = random.randrange(99999999)
    new_cur = connection.cursor()

    execution_data = []
    for item in list_of_items:
        execution_data.append((item, transaction_key))
        # execution_data.append((item, transaction_key, insert_time))
    query = """INSERT INTO ATLAS_PANDABIGMON.TMP_IDS1Debug(ID,TRANSACTIONKEY) VALUES (%s,%s)"""
    new_cur.executemany(query, execution_data)

    return transaction_key


def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
        ]


def is_timestamp(key):
    if key in ('creationtime', 'endtime', 'modificationtime', 'proddbupdatetime', 'starttime', 'statechangetime',
                    'creationdate', 'frozentime', 'ttcrequested', 'submittime', 'lastupdate'):
        return True
    return False


def parse_datetime(datetime_str):
    """
    :param datetime_str: datetime str in any format
    :return: datetime value
    """
    try:
        datetime_val = parse(datetime_str)
    except ValueError:
        datetime_val = datetime.utcfromtimestamp(datetime_str)
    return datetime_val


def is_eventservice_request(request):
    """
    :param request:
    :return: True or False
    """
    eventservice = False
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
        eventservice = True
    if 'eventservice' in request.session['requestParams'] and (
            request.session['requestParams']['eventservice'] == 'eventservice' or request.session['requestParams'][
        'eventservice'] == '1' or request.session['requestParams']['eventservice'] == '4' or
            request.session['requestParams']['eventservice'] == 'jumbo'):
        eventservice = True
    elif 'eventservice' in request.session['requestParams'] and (
            '1' in request.session['requestParams']['eventservice'] or '2' in request.session['requestParams'][
        'eventservice'] or
            '4' in request.session['requestParams']['eventservice'] or '5' in request.session['requestParams'][
                'eventservice']):
        eventservice = True
    return eventservice


def produce_objects_sample(objecttype, query, extra):
    """
    Create a sample of objects, storing all suitable objects ids in tmp table for further usage
    :param objecttype: str identifier of object type (job, task, etc)
    :param query: query dict for .filter method of ORM
    :param extra: str of extra where clause for .extra method of ORM
    :return: list of timestamps for histogram, transaction key for sample identifying, updated extra where clause
    """
    objects = []
    timestamps = []
    if objecttype == 'job':
        objects, timestamps = get_jobs_ids(query, extra)

    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
    else:
        tmpTableName = "TMP_IDS1DEBUG"

    transaction_key = insert_to_temp_table(objects)

    # form an extra query condition to exclude retried pandaids from selection
    if objecttype == 'job':
        extra += " AND pandaid not in ( select id from {0} where TRANSACTIONKEY = {1})".format(tmpTableName,
                                                                                               transaction_key)

    return timestamps, transaction_key, extra


def get_jobs_ids(query, extra):
    """
    Getting list of pandaids and modification times for job sample
    :param query: query dict for .filter method of ORM
    :param extra: str of extra where clause for .extra method of ORM
    :return: list of ids and list of modification timestamps
    """
    newquery = copy.deepcopy(query)

    pandaids = []
    pandaids.extend(Jobsdefined4.objects.filter(**newquery).extra(where=[extra]).values('pandaid', 'modificationtime'))
    pandaids.extend(Jobswaiting4.objects.filter(**newquery).extra(where=[extra]).values('pandaid', 'modificationtime'))
    pandaids.extend(Jobsactive4.objects.filter(**newquery).extra(where=[extra]).values('pandaid', 'modificationtime'))
    pandaids.extend(Jobsarchived4.objects.filter(**newquery).extra(where=[extra]).values('pandaid', 'modificationtime'))
    if parse_datetime(query['modificationtime__castdate__range'][0]) < datetime.now() - timedelta(days=3):
        pandaids.extend(Jobsarchived.objects.filter(**newquery).extra(where=[extra]).values('pandaid', 'modificationtime'))

    ids_list = []
    modtimes_list = []
    for pid in pandaids:
        ids_list.append(pid['pandaid'])
        modtimes_list.append(pid['modificationtime'])

    return ids_list, modtimes_list

