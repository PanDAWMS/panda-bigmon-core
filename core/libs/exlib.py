from core.common.models import JediDatasets, JediDatasetContents, Filestable4, FilestableArch
import math, random, datetime
from django.db import connection
from dateutil.parser import parse
from datetime import datetime
from core.settings.local import dbaccess

import core.constants as const


def drop_duplicates(object_list, **kwargs):
    """
    Dropping duplicates base on ID. By default id = pandaid.
    :param object_list: list of dicts
    :param kwargs: id: name of id param
    :return: unique_object_list: list of dicts
    """
    id_param = 'pandaid'
    if 'id' in kwargs:
        id_param = kwargs['id']

    object_dict = {}
    unique_object_list = []
    for obj in object_list:
        id = obj[id_param]
        drop_flag = False
        if id in object_dict:
            # This is a duplicate. Drop it.
            drop_flag = True
        else:
            object_dict[id] = 1
        if not drop_flag:
            unique_object_list.append(obj)

    return unique_object_list


def add_job_category(jobs):
    """
    Determine which category job belong to among: build, run or merge and add 'category' param to dict of a job
    Need 'processingtype', 'eventservice' and 'transformation' params to make a decision
    :param jobs: list of dicts
    :return: jobs: list of updated dicts
    """

    for job in jobs:
        if 'transformation' in job and 'build' in job['transformation']:
            job['category'] = 'build'
        elif 'processingtype' in job and job['processingtype'] == 'pmerge':
            job['category'] = 'merge'
        elif 'eventservice' in job and (job['eventservice'] == 2 or job['eventservice'] == 'esmerge'):
            job['category'] = 'merge'
        else:
            job['category'] = 'run'

    return jobs


def job_states_count_by_param(jobs, **kwargs):
    """
    Counting jobs in different states and group by provided param
    :param jobs:
    :param kwargs:
    :return:
    """
    param = 'category'
    if 'param' in kwargs:
        param = kwargs['param']

    job_states_count_dict = {}
    param_values = list(set([job[param] for job in jobs if param in job]))

    if len(param_values) > 0:
        for pv in param_values:
            job_states_count_dict[pv] = {}
            for state in const.JOB_STATES:
                job_states_count_dict[pv][state] = 0

    for job in jobs:
        job_states_count_dict[job[param]][job['jobstatus']] += 1

    job_summary_dict = {}
    for pv, data in job_states_count_dict.items():
        if pv not in job_summary_dict:
            job_summary_dict[pv] = []

            for state in const.JOB_STATES:
                statecount = {
                    'name': state,
                    'count': job_states_count_dict[pv][state],
                }
                job_summary_dict[pv].append(statecount)

    # dict -> list
    job_summary_list = []
    for key, val in job_summary_dict.items():
        tmp_dict = {
            'param': param,
            'value': key,
            'job_state_counts': val,
        }
        job_summary_list.append(tmp_dict)

    return job_summary_list


def getDataSetsForATask(taskid, type = None):
    query = {
        'jeditaskid': taskid
    }
    if type:
        query['type']=type
    ret = []
    dsets = JediDatasets.objects.filter(**query).values()
    for dset in dsets:
        ret.append({
            'datasetname': dset['datasetname'],
            'type': dset['type']
        })
    return ret




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


def get_file_info(job_list, **kwargs):
    """
    Enrich job_list dicts by file information. By default: filename (lfn) and size
    :param job_list: list of dicts
    :return: job_list
    """
    file_info = []
    fquery = {}
    if 'type' in kwargs and kwargs['type']:
        fquery['type'] = kwargs['type']
    is_archive = False
    if 'is_archive' in kwargs and kwargs['is_archive']:
        is_archive = kwargs['is_archive']
    fvalues = ('pandaid', 'type', 'lfn', 'fsize')

    pandaids = []
    if len(job_list) > 0:
        pandaids.extend([job['pandaid'] for job in job_list if 'pandaid' in job and job['pandaid']])

    if len(pandaids) > 0:
        tk = insert_to_temp_table(pandaids)
        extra = "pandaid in (select ID from {} where TRANSACTIONKEY = {})".format(get_tmp_table_name(), tk)

        file_info.extend(Filestable4.objects.filter(**fquery).extra(where=[extra]).values(*fvalues))

        if is_archive:
            file_info.extend(FilestableArch.objects.filter(**fquery).extra(where=[extra]).values(*fvalues))

    file_info_dict = {}
    if len(file_info) > 0:
        for file in file_info:
            if file['pandaid'] not in file_info_dict:
                file_info_dict[file['pandaid']] = []
            file_info_dict[file['pandaid']].append(file)

        for job in job_list:
            if job['pandaid'] in file_info_dict:
                for file in file_info_dict[job['pandaid']]:
                    if file['type'] + 'filename' not in job:
                        job[file['type'] + 'filename'] = ''
                        job[file['type'] + 'filesize'] = 0
                    job[file['type'] + 'filename'] += file['lfn'] + ','
                    job[file['type'] + 'filesize'] += file['fsize'] if isinstance(file['fsize'], int) else 0

    return job_list


def insert_to_temp_table(list_of_items, transactionKey = -1):
    """Inserting to temp table
    :param list_of_items
    :return transactionKey and timestamp of instering
    """

    tmpTableName = get_tmp_table_name()

    if transactionKey == -1:
        random.seed()
        transactionKey = random.randrange(1000000)

    new_cur = connection.cursor()
    executionData = []
    for item in list_of_items:
        executionData.append((item, transactionKey))
    query = """INSERT INTO {}(ID,TRANSACTIONKEY) VALUES (%s,%s)""".format(tmpTableName)
    new_cur.executemany(query, executionData)

    return transactionKey


def get_event_status_summary(pandaids, eventservicestatelist):
    """
    Getting event statuses summary for list of pandaids of ES jobs
    :param pandaids: list
    :return: dict of status: nevents
    """
    summary = {}

    tmpTableName = get_tmp_table_name()

    transactionKey = random.randrange(1000000)

    new_cur = connection.cursor()
    executionData = []
    for id in pandaids:
        executionData.append((id, transactionKey))
    query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
    new_cur.executemany(query, executionData)

    new_cur.execute(
        """
        SELECT STATUS, COUNT(STATUS) AS COUNTSTAT 
        FROM (
            SELECT /*+ dynamic_sampling(TMP_IDS1 0) cardinality(TMP_IDS1 10) INDEX_RS_ASC(ev JEDI_EVENTS_PANDAID_STATUS_IDX) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */ PANDAID, STATUS 
            FROM ATLAS_PANDA.JEDI_EVENTS ev, %s 
            WHERE TRANSACTIONKEY = %i AND  PANDAID = ID
        ) t1 
        GROUP BY STATUS""" % (tmpTableName, transactionKey))

    evtable = dictfetchall(new_cur)

    for ev in evtable:
        evstat = eventservicestatelist[ev['STATUS']]
        summary[evstat] = ev['COUNTSTAT']

    return summary



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


def lower_string(string):
    return string.lower() if isinstance(string, str) else string


def lower_dicts_in_list(input_list):
    output_list = []
    for row_dict in input_list:
        out_dict = {lower_string(k): lower_string(v) for k,v in row_dict.items()}
        output_list.append(out_dict)
    return output_list


def get_job_queuetime(job):
    """
    :param job: dict of job params, starttime and creationtime is obligatory
    :return: queuetime in seconds or None if not enough data provided
    """
    queueutime = None

    if 'starttime' in job and job['starttime'] is not None:
        starttime = parse_datetime(job['starttime']) if not isinstance(job['starttime'], datetime) else job['starttime']
    else:
        starttime = None
    if 'endtime' in job and job['endtime'] is not None:
        creationtime = parse_datetime(job['creationtime']) if not isinstance(job['creationtime'], datetime) else job['creationtime']
    else:
        creationtime = None

    if starttime and creationtime:
        queueutime = (starttime-creationtime).total_seconds()

    return queueutime
