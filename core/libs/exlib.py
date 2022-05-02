
import math
import random
import numpy as np
import pandas as pd
from datetime import timedelta
from django.db import connection

from core.common.models import JediDatasets, JediDatasetContents, Filestable4, FilestableArch, Sitedata
from core.settings.config import DB_SCHEMA, DEPLOYMENT


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


def getDataSetsForATask(taskid, type=None):
    query = {
        'jeditaskid': taskid
    }
    if type:
        query['type'] = type
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
                    else:
                        njob[jds['pandaid']]['nevents'] = int(jds['nevents'])
                if jds['type'] == 'input':
                    njob[jds['pandaid']]['ninputs'] = []
                    njob[jds['pandaid']]['ninputs'].append(jds['lfn'])
            else:
                if jds['endevent'] != None and jds['startevent'] != None:
                    njob[jds['pandaid']]['nevents'] += int(jds['endevent']) + 1 - int(jds['startevent'])
                else:
                    if jds['nevents'] != None:
                        njob[jds['pandaid']]['nevents'] += int(jds['nevents'])
                    else:
                        njob[jds['pandaid']]['nevents'] += 0
                if jds['type'] =='input':
                    njob[jds['pandaid']]['ninputs'].append(jds['lfn'])
        listpandaidsDS = njob.keys()
        listpandaidsF4 = pandaLFN.keys()
        for job in jobs:
            if job['pandaid'] in listpandaidsDS:
                job['nevents'] = njob[job['pandaid']]['nevents']
                if 'ninputs' in njob[job['pandaid']]:
                    job['ninputs'] = len(njob[job['pandaid']]['ninputs'])
                else:
                    job['ninputs'] = 0
                    if job['processingtype'] == 'pmerge':
                        if len(job['jobinfo']) == 0:
                            job['jobinfo'] = 'Pmerge job'
                        else:
                            job['jobinfo'] += 'Pmerge job'
            else:
                if job['pandaid'] in listpandaidsF4:
                    job['ninputs'] = len(pandaLFN[job['pandaid']])
                else: job['ninputs'] = 0
            if job['pandaid'] in listpandaidsF4 and 'ninputs' not in job:
                 job['ninputs'] = len(pandaLFN[job['pandaid']])
            if job['nevents'] != 0 and 'ninputs' not in job and job['pandaid'] in listpandaidsF4:
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
    if DEPLOYMENT == "POSTGRES":
        create_temporary_table(new_cur, tmpTableName)
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


def dictfetchall(cursor, **kwargs):
    "Returns all rows from a cursor as a dict"
    style = 'default'
    if 'style' in kwargs:
        style = kwargs['style']
    desc = cursor.description
    if style == 'uppercase':
        return [
            dict(zip([str(col[0]).upper() for col in desc], row))
            for row in cursor.fetchall()
        ]
    else:
        return [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
            ]


def is_timestamp(key):
    if key in ('creationtime', 'endtime', 'modificationtime', 'proddbupdatetime', 'starttime', 'statechangetime',
                    'creationdate', 'frozentime', 'ttcrequested', 'submittime', 'lastupdate'):
        return True
    return False


def get_tmp_table_name():
    tmpTableName = f"{DB_SCHEMA}.TMP_IDS1"
    if DEPLOYMENT == 'POSTGRES':
        tmpTableName = "TMP_IDS1"
    return tmpTableName


def get_tmp_table_name_debug():
    tmpTableName = f"{DB_SCHEMA}.TMP_IDS1DEBUG"
    return tmpTableName


def create_temporary_table(cursor, tmpTableName):
    # Postgres does not keep the temporary table definition across connections, this is why we should recreate them
    sql_query = f"""
    CREATE TEMPORARY TABLE if not exists {tmpTableName} 
    ("id" bigint, "transactionkey" bigint) ON COMMIT PRESERVE ROWS;
    COMMIT;
    """
    cursor.execute(sql_query)


def lower_string(string):
    return string.lower() if isinstance(string, str) else string


def lower_dicts_in_list(input_list):
    output_list = []
    for row_dict in input_list:
        out_dict = {lower_string(k): lower_string(v) for k,v in row_dict.items()}
        output_list.append(out_dict)
    return output_list


def convert_bytes(n_bytes, output_unit='MB'):
    """
    Convert bytes to KB, MB etc
    :param n_bytes: int
    :param output_unit: str
    :return: output
    """
    output = 0
    multipliers_dict = {
        'KB': 1.0/1000,
        'MB': 1.0/1000000,
        'GB': 1.0/1000000000,
        'TB': 1.0/1000000000000,
        'KiB': 1.0/1024,
        'MiB': 1.0/1024/1024,
        'GiB': 1.0/1024/1024/1024,
        'TiB': 1.0/1024/1024/1024/1024,
    }
    if output_unit in multipliers_dict.keys():
        output = n_bytes*multipliers_dict[output_unit]

    return output


def convert_hs06(input, unit):
    """
    taking into account cputimeunit
    :param cputime: int
    :param unit: str
    :return:
    """
    output = 0
    multipliers_dict = {
        'HS06sPerEvent': 1,
        'mHS06sPerEvent': 1.0/1000,
    }
    if unit in multipliers_dict:
        output = input * multipliers_dict[unit]

    return output


def convert_sec(duration_sec):
    """Convert seconds to dd:hh:mm:ss str"""
    duration_str = '-'
    if duration_sec is not None and duration_sec > 0:
        duration_str = str(timedelta(seconds=duration_sec)).split('.')[0]
        if 'day' in duration_str:
            duration_str = duration_str.replace(' day, ', ':')
            duration_str = duration_str.replace(' days, ', ':')
        else:
            duration_str = '0:' + duration_str

    return duration_str


def split_into_intervals(input_data, **kwargs):
    """
    Split numeric values list into intervals for sumd
    :param input_data: list
    :return: output_data: list of dicts
    """
    N_BIN_MAX = 20
    minstep = 1
    if 'minstep' in kwargs and kwargs['minstep'] and isinstance(kwargs['minstep'], int):
        minstep = kwargs['minstep']

    output_data = []
    data_dict = {}
    if isinstance(input_data, list):
        for v in input_data:
            if v not in data_dict:
                data_dict[v] = 0
            data_dict[v] += 1

    kys = list(data_dict.keys())

    # find range bounds
    rangebounds = []
    if min(kys) == 0:
        output_data.append({'kname': '0-0', 'kvalue': data_dict[0]})
        dstep = minstep if (max(kys) - min(kys) + 1) / N_BIN_MAX < minstep else int(round_to_n((max(kys) - min(kys) + 1) / N_BIN_MAX, 1))
        rangebounds.extend([lb for lb in range(min(kys) + 1, max(kys) + dstep, dstep)])
    else:
        dstep = minstep if (max(kys) - min(kys)) / N_BIN_MAX < minstep else int(round_to_n((max(kys) - min(kys)) / N_BIN_MAX, 1))
        rangebounds.extend([lb - 1 for lb in range(min(kys), max(kys) + dstep, dstep)])
    if len(rangebounds) == 1:
        rangebounds.append(rangebounds[0] + dstep)

    # split list into calculated ranges
    bins, ranges = np.histogram(input_data, bins=rangebounds)
    for i, bin in enumerate(bins):
        if bin != 0:
            output_data.append({'kname': str(ranges[i]) + '-' + str(ranges[i + 1]), 'kvalue': bin})

    return output_data


def build_stack_histogram(data_raw, **kwargs):
    """
    Prepare stack histogram data and calculate mean and std metrics
    :param data_raw: dict of lists
    :param kwargs:
    :return:
    """

    n_decimals = 0
    if 'n_decimals' in kwargs:
        n_decimals = kwargs['n_decimals']

    N_BINS_MAX = 50
    if 'n_bin_max' in kwargs:
        N_BINS_MAX = kwargs['n_bin_max']
    stats = []
    columns = []

    data_all = []
    for site, sd in data_raw.items():
        data_all.extend(sd)

    stats.append(np.average(data_all) if not np.isnan(np.average(data_all)) else 0)
    stats.append(np.std(data_all) if not np.isnan(np.std(data_all)) else 0)

    bins_all, ranges_all = np.histogram(data_all, bins='auto')
    if len(ranges_all) > N_BINS_MAX + 1:
        bins_all, ranges_all = np.histogram(data_all, bins=N_BINS_MAX)
    ranges_all = list(np.round(ranges_all, n_decimals))

    x_axis_ticks = ['x']
    x_axis_ticks.extend(ranges_all[:-1])

    for stack_param, data in data_raw.items():
        column = [stack_param]
        column.extend(list(np.histogram(data, ranges_all)[0]))
        # do not add if all the values are zeros
        if sum(column[1:]) > 0:
            columns.append(column)

    # sort by biggest impact
    columns = sorted(columns, key=lambda x: sum(x[1:]), reverse=True)

    columns.insert(0, x_axis_ticks)

    return stats, columns


def build_time_histogram(data):
    """
    Preparing data for time-based histogram.
    :param data: list. if 1xN - counting occurances, if 2xN - sum for each occurance
    :return:
    """
    N_BINS_MAX = 60
    agg = 'count'
    if len(data) > 0 and isinstance(data[0], list) and len(data[0]) == 2:
        agg = 'sum'

    # find optimal interval
    if agg == 'count':
        timestamp_list = data
    else:
        timestamp_list = [item[0] for item in data]

    full_timerange_seconds = (max(timestamp_list) - min(timestamp_list)).total_seconds()

    step = 30
    label = 'S'
    while full_timerange_seconds/step > N_BINS_MAX:
        if step <= 600:
            step += 30
        elif step <= 3600:
            step += 600
            label = 'T'
        elif step <= 3600 * 24:
            step += 3600
            label = 'H'
        elif step <= 3600 * 24 * 7:
            step += 3600 * 24
            label = 'D'
        elif step <= 3600 * 24 * 30:
            step += 3600 * 24 * 7
            label = 'W'
        else:
            step += 3600 * 24 * 30
            label = 'M'

    labels = {
        'S': 1,
        'T': 60,
        'H': 3600,
        'D': 3600*24,
        'W': 3600 * 24 * 7,
        'M': 3600 * 24 * 30,
    }
    freq = '{}{}'.format(math.floor(step/labels[label]), label)

    # prepare binned data
    if agg == 'count':
        df = pd.DataFrame(timestamp_list, columns=['date'])
        df.set_index('date', drop=False, inplace=True)
        binned_data = df.groupby(pd.Grouper(freq=freq)).count()
    else:
        df = pd.DataFrame(data, columns=['date', 'value'])
        df.set_index('date', drop=False, inplace=True)
        binned_data = df.groupby(pd.Grouper(key='date', freq=freq)).sum()

    data = []
    index = binned_data.index.to_pydatetime().tolist()
    for i, item in enumerate(binned_data.values.tolist()):
        data.append([index[i], item])

    return data


def count_occurrences(obj_list, params_to_count, output='dict'):
    """
    Count occurrences of each param value for list of dicts
    :param obj_list:
    :param params_to_count:
    :param output: str (list or dict). list - for plots
    :return: param_counts: dict
    """
    param_counts = {}

    for obj in obj_list:
        for p in params_to_count:
            if p in obj and obj[p] is not None and obj[p] != '':
                if p not in param_counts:
                    param_counts[p] = {}
                if obj[p] not in param_counts[p]:
                    param_counts[p][obj[p]] = 0
                param_counts[p][obj[p]] += 1

    if output == 'list':
        for p in param_counts:
            param_counts[p] = [[v, c] for v, c in param_counts[p].items()]
            param_counts[p] = sorted(param_counts[p], key=lambda x: x[1], reverse=True)

    return param_counts


def duration_df(data_raw, id_name='JEDITASKID', timestamp_name='MODIFICATIONTIME'):
    """
    Calculates duration of each status by modificationtime delta
    (in days)
    """
    task_states_duration = {}
    if len(data_raw) > 0:
        df = pd.DataFrame(data_raw)
        groups = df.groupby([id_name])
        for k, v in groups:
            v.sort_values(by=[timestamp_name], inplace=True)
            v['START_TS'] = pd.to_datetime(v[timestamp_name])
            v['END_TS'] = v['START_TS'].shift(-1).fillna(v['START_TS'])
            v['DURATION'] = (v['END_TS'] - v['START_TS']).dt.total_seconds()/60./60./24.
            task_states_duration[k] = v.groupby(['status'])['DURATION'].sum().to_dict()

    return task_states_duration


def round_to_n(x, n):
    if not x:
        return 0
    power = - int(math.floor(math.log10(abs(x)))) + (n - 1)
    factor = (10 ** power)

    return round(x * factor) / factor


def getPilotCounts(view):
    """ Getting pilots counts by PQ. """
    query = {
        # 'flag': view,
        'hours': 3,
    }
    job_values = ('getjob', 'updatejob', 'nojob', 'getjobabs', 'updatejobabs', 'nojobabs')
    values = ('site', 'lastmod') + job_values
    rows = list(Sitedata.objects.filter(**query).values(*values))
    pilotd = {}
    if len(rows) > 0:
        for r in rows:
            site = r['site']
            if not site in pilotd:
                pilotd[site] = {}
            for jb in job_values:
                pilotd[site]['count_' + jb] = r[jb]
            pilotd[site]['count'] = r['getjob']
            pilotd[site]['count_abs'] = r['getjobabs']
            pilotd[site]['time'] = r['lastmod']

    return pilotd