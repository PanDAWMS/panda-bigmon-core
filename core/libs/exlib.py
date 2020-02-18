from core.common.models import JediDatasets, JediDatasetContents, Filestable4, FilestableArch
import math, random, datetime
from django.db import connection
from dateutil.parser import parse
from datetime import datetime
from core.settings.local import dbaccess


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


def insert_to_temp_table(list_of_items, transactionKey = -1):
    """Inserting to temp table
    :param list_of_items
    :return transactionKey and timestamp of instering
    """

    # if dbaccess['default']['ENGINE'].find('oracle') >= 0:
    #     tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    # else:
    #     tmpTableName = "TMP_IDS1"

    if transactionKey == -1:
        random.seed()
        transactionKey = random.randrange(1000000)

    new_cur = connection.cursor()
    executionData = []
    for item in list_of_items:
        executionData.append((item, transactionKey))
    query = """INSERT INTO ATLAS_PANDABIGMON.TMP_IDS1Debug(ID,TRANSACTIONKEY) VALUES (%s,%s)"""
    new_cur.executemany(query, executionData)

    return transactionKey


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


def get_job_walltime(job):
    """
    :param job: dict of job params, starttime and endtime is obligatory;
                creationdate, statechangetime, and modificationtime are optional
    :return: walltime in seconds or None if not enough data provided
    """
    walltime = None

    if 'endtime' in job and job['endtime'] is not None:
        endtime = parse_datetime(job['endtime']) if not isinstance(job['endtime'], datetime) else job['endtime']
    elif 'statechangetime' in job and job['statechangetime'] is not None:
        endtime = parse_datetime(job['statechangetime']) if not isinstance(job['statechangetime'], datetime) else job['statechangetime']
    elif 'modificationtime' in job and job['modificationtime'] is not None:
        endtime = parse_datetime(job['modificationtime']) if not isinstance(job['modificationtime'], datetime) else job['modificationtime']
    else:
        endtime = None

    if 'starttime' in job and job['starttime'] is not None:
        starttime = parse_datetime(job['starttime']) if not isinstance(job['starttime'], datetime) else job['starttime']
    elif 'creationdate' in job and job['creationdate'] is not None:
        starttime = parse_datetime(job['creationdate']) if not isinstance(job['creationdate'], datetime) else job['creationdate']
    else:
        starttime = 0

    if starttime and endtime:
        walltime = (endtime-starttime).total_seconds()

    return walltime


def is_job_active(jobststus):
    """
    Check if jobstatus is one of the active
    :param jobststus: str
    :return: True or False
    """
    end_status_list = ['finished', 'failed', 'cancelled', 'closed']
    if jobststus in end_status_list:
        return False

    return True

def get_tmp_table_name():
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        tmpTableName = "TMP_IDS1"
    return tmpTableName
