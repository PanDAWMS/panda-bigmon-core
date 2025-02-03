import datetime
import math
import random
import numpy as np
import pandas as pd
from datetime import timedelta
from django.db import connection

from core.common.models import JediDatasets, Filestable4, FilestableArch, Sitedata, ResourceTypes
from core.schedresource.utils import get_panda_queues
from django.conf import settings


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
    if settings.DEPLOYMENT == "POSTGRES":
        create_temporary_table(new_cur, tmpTableName)
    executionData = []
    for item in list_of_items:
        executionData.append((item, transactionKey))
    query = """INSERT INTO {}(ID,TRANSACTIONKEY) VALUES (%s,%s)""".format(tmpTableName)
    new_cur.executemany(query, executionData)

    return transactionKey


def dictfetchall(cursor, **kwargs):
    """Returns all rows from a cursor as a dict"""
    style = 'default'
    if 'style' in kwargs:
        style = kwargs['style']
    desc = cursor.description
    if style == 'uppercase':
        return [
            dict(zip([str(col[0]).upper() for col in desc], row))
            for row in cursor.fetchall()
        ]
    elif style == 'lowercase':
        return [
            dict(zip([str(col[0]).lower() for col in desc], row))
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
    tmpTableName = f"{settings.DB_SCHEMA}.tmp_ids1"
    if settings.DEPLOYMENT == 'POSTGRES':
        tmpTableName = "tmp_ids1"
    return tmpTableName


def get_tmp_table_name_debug():
    tmpTableName = f"{settings.DB_SCHEMA}.tmp_ids1debug"
    return tmpTableName


def create_temporary_table(cursor, tmpTableName):
    # Postgres does not keep the temporary table definition across connections, this is why we should recreate them
    sql_query = f"""
    create temporary table if not exists {tmpTableName} 
    ("id" bigint, "transactionkey" bigint) on commit preserve rows;
    commit;
    """
    cursor.execute(sql_query)


def lower_string(string):
    return string.lower() if isinstance(string, str) else string


def lower_dicts_in_list(input_list):
    output_list = []
    for row_dict in input_list:
        out_dict = {lower_string(k): v for k, v in row_dict.items()}
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


def convert_sec(duration_sec, out_unit='str', **kwargs):
    """
    Convert seconds to minutes, hours etc., or str format 'dd:hh:mm:ss'
    :param duration_sec: int: n seconds
    :param out_unit: str: unit of output, str is default
    :return output
    """
    output = None
    if 'n_round_digits' in kwargs and kwargs['n_round_digits'] and isinstance(kwargs['n_round_digits'], int):
        n_round_digits = kwargs['n_round_digits']
    else:
        n_round_digits = 0

    multipliers_dict = {
        'sec': 1.0,
        'min': 1.0/60,
        'hour': 1.0/60/60,
        'day': 1.0/60/60/24,
        'week': 1.0/60/60/24/7,
        'month': 1.0/60/60/24/7/30,
        'year': 1.0/(60*60*24*7*30*12 + 5),
    }

    if duration_sec is not None and duration_sec >= 0:
        if out_unit in multipliers_dict:
            output = round_to_n_digits(duration_sec * multipliers_dict[out_unit], n_round_digits)
        elif out_unit == 'str':
            output = str(timedelta(seconds=duration_sec)).split('.')[0]
            if 'day' in output:
                output = output.replace(' day, ', ':')
                output = output.replace(' days, ', ':')
            else:
                output = '0:' + output

    return output

def convert_epoch_to_datetime(timestamp):
    """
    Converting epoch to datetime + checking if it is in milliseconds or seconds
    :param timestamp: int
    :return: output: datetime in UTC
    """
    output = None
    if isinstance(timestamp, int) and timestamp > 0:
        if len(str(timestamp)) >= 13:
            # it seems the input is in milliseconds -> convert to seconds
            timestamp = int(timestamp/1000.)
        output = datetime.datetime.utcfromtimestamp(timestamp)

    return output


def convert_grams(n_grams, output_unit='auto'):
    """
    Convert grams to kg, tonne etc. If output_unit is "auto", return value and selected unit
    :param n_grams: int
    :param output_unit: str
    :return: output
    :return: output_unit
    """
    output = float(0)
    if (isinstance(n_grams, int) or isinstance(n_grams, float)) and n_grams > 0:
        n_grams = float(n_grams)
        multipliers_dict = {
            'pg': float(1000000000000),
            'ng': float(1000000000),
            'µg': float(1000000),
            'mg': float(1000),
            'g': float(1),
            'kg': float(1.0/1000),
            't': float(1.0/1000000),
            'Mt': float(1.0/1000000000),
            'Gt': float(1.0/1000000000000),
        }
        if output_unit in multipliers_dict.keys():
            output = n_grams*multipliers_dict[output_unit]
        elif output_unit == 'auto':
            for unit, mp in multipliers_dict.items():
                if 1.0 <= n_grams * mp < 1000.0:
                    output = n_grams * mp
                    output_unit = unit
                    break
    else:
        output_unit = 'g'

    return output, output_unit


def convert_to_si_prefix(input_value, output_unit='auto'):
    """
    Convert value to desired SI prefix. If output_unit is "auto", rounds to significant figures and
    :param input_value: int or float
    :param output_unit: str - SI prefix: G, M, k, m, µ, ... or "auto"
    :return: output: float
    :return: output_unit: str
    """
    output = float(0)
    if (isinstance(input_value, int) or isinstance(input_value, float)) and input_value > 0:
        input_value = float(input_value)
        multipliers_dict = {
            'a': float(1000000000000000000),
            'f': float(1000000000000000),
            'p': float(1000000000000),
            'n': float(1000000000),
            'µ': float(1000000),
            'm': float(1000),
            '': float(1),
            'k': float(1.0 / 1000),
            'M': float(1.0 / 1000000),
            'G': float(1.0 / 1000000000),
            'T': float(1.0 / 1000000000000),
            'P': float(1.0 / 1000000000000000),
            'E': float(1.0 / 1000000000000000000),
        }
        if output_unit in multipliers_dict:
            output = input_value * multipliers_dict[output_unit]
        elif output_unit == 'auto':
            for unit, mp in multipliers_dict.items():
                if 1.0 <= input_value * mp < 1000.0:
                    output = input_value * mp
                    output_unit = unit
                    break
    else:
        output_unit = ''

    return output, output_unit




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


def calc_nbins(length, n_bins_max=50):
    """
    Calculate N bins depending on length of data.
    It is needed as np.histogram(data, bins='auto') uses extreme amount of memory in case of outliers.
    :param length: length of data to be binned
    :return: n_bins
    """

    n_bins = 1
    if length is not None and isinstance(length, int) and length > 0:
        n_bins = math.ceil(pow(length, 1/1.75))
    if n_bins > n_bins_max:
        n_bins = n_bins_max

    return n_bins


def calc_freq_time_series(timestamp_list, n_bins_max=60):
    """
    Calculate N bins for time series data
    :param timestamp_list:
    :param n_bins_max:
    :return: freq: str - for data frame grouping
    """
    if len(timestamp_list) == 0:
        return '10T'
    full_timerange_seconds = (max(timestamp_list) - min(timestamp_list)).total_seconds()

    step = 30
    label = 'S'
    while full_timerange_seconds/step > n_bins_max:
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
    return freq


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

    n_bins_max = 50
    if 'n_bin_max' in kwargs:
        n_bins_max = kwargs['n_bin_max']
    stats = []
    columns = []

    data_all = []
    for site, sd in data_raw.items():
        data_all.extend(sd)

    stats.append(round_to_n_digits(np.average(data_all), 3) if not np.isnan(np.average(data_all)) else 0)
    stats.append(round_to_n_digits(np.std(data_all), 3) if not np.isnan(np.std(data_all)) else 0)

    # if std less than n decimals, i.e. not enough resolution - set n_bins to 1
    if stats[1] <= 1/(10**n_decimals):
        n_bins = 1
    else:
        n_bins = calc_nbins(len(data_all), n_bins_max)
    bins_all, ranges_all = np.histogram(data_all, bins=n_bins)

    # calc x-axis ticks, get average from each range
    x_axis_ticks = ['x']
    ranges_all_avg = np.convolve(ranges_all, np.ones(2), 'valid') / 2
    x_axis_ticks.extend([round_to_n_digits(r, n_decimals) for r in list(ranges_all_avg)])

    ranges_all = list(ranges_all)

    for stack_param, data in data_raw.items():
        column = [stack_param]
        column.extend([int(r) for r in list(np.histogram(data, ranges_all)[0])])
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
    agg = 'count'
    if len(data) > 0 and isinstance(data[0], list) and len(data[0]) == 2:
        agg = 'sum'

    # find optimal interval
    if agg == 'count':
        timestamp_list = data
    else:
        timestamp_list = [item[0] for item in data]
    freq = calc_freq_time_series(timestamp_list, n_bins_max=60)

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


def group_low_occurrences(data, threshold=0.01):
    """
    Group low occurrences into "other" category
    :param data: list of lists
    :param threshold: float (0-1)
    :return: data: list of lists
    """
    grouped_data = []
    n_other = 0
    if len(data) > 0:
        total = sum([item[1] for item in data])
        if total > 0:
            for item in data:
                if item[1] / total > threshold:
                    grouped_data.append(item)
                else:
                    n_other += item[1]
            if n_other > 0:
                grouped_data.append(['other', n_other])
    return grouped_data



def duration_df(data_raw, id_name='jeditaskid', timestamp_name='modificationtime'):
    """
    Calculates duration of each status by modificationtime delta
    (in days)
    """
    task_states_duration = {}
    if len(data_raw) > 0:
        df = pd.DataFrame(data_raw)
        groups = df.groupby(id_name)
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


def round_to_n_digits(x, n=0, method='normal'):
    """
    Round float to n decimals.
    :param x: float number
    :param n: decimals
    :param method: str: normal, up or down
    :return:
    """
    if not x:
        return 0

    factor = (10 ** n)

    if method == 'normal':
        x = round(x * factor) / factor
    elif method == 'ceil':
        x = math.ceil(x * factor) / factor
    elif method == 'floor':
        x = math.floor(x * factor) / factor

    if n == 0:
        x = int(x)

    return x


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


def get_resource_types():
    """
    Get resource types from DB
    :return: resource_types: list
    """
    resource_types = list(ResourceTypes.objects.all().values())
    return resource_types

def get_maxrampercore_dict():
    """
    Get maxrampercore values depending on resource type and computingsite
    :return:
    """
    resource_types = get_resource_types()
    pqs = get_panda_queues()
    maxrampercore_dict = {}
    for rt in resource_types:
        if rt['resource_name'] not in maxrampercore_dict:
            maxrampercore_dict[rt['resource_name']] = {}
        for pq, pq_data in pqs.items():
            if rt['maxrampercore'] is not None:
                maxrampercore_dict[rt['resource_name']][pq] = int(rt['maxrampercore'])
            elif rt['maxrampercore'] is None and pq_data['maxrss'] is not None and pq_data['corecount'] is not None:
                maxrampercore_dict[rt['resource_name']][pq] = int(pq_data['maxrss']/pq_data['corecount'])

    return maxrampercore_dict