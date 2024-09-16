"""
    status_summary.utils
    
"""
import logging
import pytz
from datetime import datetime, timedelta, timezone

from core.schedresource.utils import get_panda_queues
import core.constants as const

_logger = logging.getLogger('bigpandamon')

defaultDatetimeFormat = '%Y-%m-%dT%H:%M:%S'


def configure(request_GET):
    errors_GET = {}
    ### if starttime&endtime are provided, use them
    if 'starttime' in request_GET and 'endtime' in request_GET:
        nhours = -1
        ### starttime
        starttime = request_GET['starttime']
        try:
            dt_start = datetime.strptime(starttime, defaultDatetimeFormat)
        except ValueError:
            errors_GET['starttime'] = \
                'Provided starttime [%s] has incorrect format, expected [%s].' % (starttime, defaultDatetimeFormat)
            starttime = datetime.now(tz=timezone.utc) - timedelta(hours=nhours)
            starttime = starttime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        ### endtime
        endtime = request_GET['endtime']
        try:
            dt_end = datetime.strptime(endtime, defaultDatetimeFormat)
        except ValueError:
            errors_GET['endtime'] = \
                'Provided endtime [%s] has incorrect format, expected [%s].' % (endtime, defaultDatetimeFormat)
            endtime = datetime.now(tz=timezone.utc)
            endtime = endtime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    ### if nhours is provided, do query "last N days"
    elif 'nhours' in request_GET:
        try:
            nhours = int(request_GET['nhours'])
        except:
            nhours = 12
            errors_GET['nhours'] = \
                'Wrong or no nhours has been provided.Using [%s].' % (nhours)
        starttime = datetime.now(tz=timezone.utc) - timedelta(hours=nhours)
        starttime = starttime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        endtime = datetime.now(tz=timezone.utc)
        endtime = endtime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    ### neither nhours, nor starttime&endtime was provided
    else:
        nhours = 12
        starttime = datetime.now(tz=timezone.utc) - timedelta(hours=nhours)
        starttime = starttime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        endtime = datetime.now(tz=timezone.utc)
        endtime = endtime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        errors_GET['noparams'] = \
                'Neither nhours, nor starttime & endtime has been provided. Using starttime=%s and endtime=%s.' % \
                (starttime, endtime)

    ### if mcp_cloud is provided, use it. comma delimited strings
    f_mcp_cloud = ''
    if 'mcp_cloud' in request_GET:
        f_mcp_cloud = request_GET['mcp_cloud']

    ### if computingsite is provided, use it. comma delimited strings
    f_computingsite = ''
    if 'computingsite' in request_GET:
        f_computingsite = request_GET['computingsite']

    ### if jobstatus is provided, use it. comma delimited strings
    f_jobstatus = ''
    if 'jobstatus' in request_GET:
        f_jobstatus = request_GET['jobstatus']

    ### if corecount is provided, use it. comma delimited strings, exclude with -N
    f_corecount = ''
    if 'corecount' in request_GET:
        f_corecount = request_GET['corecount']

    ### if jobtype is provided, use it. comma delimited strings
    f_jobtype = ''
    if 'jobtype' in request_GET:
        f_jobtype = request_GET['jobtype']

    ### if cloud is provided, use it. comma delimited strings
    f_cloud = ''
    if 'cloud' in request_GET:
        f_cloud = request_GET['cloud']

    ### if atlas_site is provided, use it. comma delimited strings
    f_atlas_site = ''
    if 'atlas_site' in request_GET:
        f_atlas_site = request_GET['atlas_site']

    ### if status is provided, use it. comma delimited strings
    f_status = ''
    if 'status' in request_GET:
        f_status = request_GET['status']

    return starttime, endtime, nhours, errors_GET, f_computingsite, f_mcp_cloud, f_jobstatus, f_corecount, f_jobtype, \
        f_cloud, f_atlas_site, f_status


def process_wildcards_str(value_list, key_base, include_flag=True):
    query = {}
    for val in value_list:
        ### NULL
        if val.upper() == 'NULL':
            key = '%s__isnull' % (key_base)
            query[key] = include_flag
        ### no wildcard, use __in
        elif val.find('*') == -1 and len(val):
            key='%s__in' % (key_base)
            if key not in query:
                query[key] = []
            query[key].append(val)
        else:
            items = val.split('*')
            if len(items) > 0:
                ### startswith
                first = items.pop(0)
                if len(first):
                    key = '%s__istartswith' % (key_base)
                    query[key] = first
                ### endswith
                if len(items) > 1:
                    last = items.pop(-1)
                    if len(last):
                        key = '%s__iendswith' % (key_base)
                        query[key] = last
                ### contains
                items_not_empty = [x for x in items if len(x)]
                if len(items_not_empty):
                    key = '%s__icontains' % (key_base)
                    query[key] = items_not_empty[0]
    return query


def parse_param_values_str(GET_param_field, key_base):
    query = {}
    exclude_query = {}

    # filter mcp_cloud
    fval = GET_param_field.split(',')
    if len(fval) and len(fval[0]):
        # get exclude values
        exclude_values = [x[1:] for x in fval if x.startswith('-')]
        # get include values
        include_values = [x for x in fval if not x.startswith('-')]
        # process wildcards
        exclude_query.update(process_wildcards_str(exclude_values, key_base, include_flag=False))
        query.update(process_wildcards_str(include_values, key_base))
    return query, exclude_query


def process_wildcards_int(value_list, key_base, include_flag=True):
    query = {}
    for val in value_list:
        ### NULL
        if val.upper() == 'NULL':
            key = '%s__isnull' % (key_base)
            query[key] = include_flag
        else:
            key = '%s__exact' % (key_base)
            query[key] = val[0]
    return query


def parse_param_values_int(GET_param_field, key_base):
    query = {}
    exclude_query = {}

    ### filter mcp_cloud
    fval = GET_param_field.split(',')
    if len(fval) and len(fval[0]):
        ### get exclude values
        exclude_values = [x[1:] for x in fval if x.startswith('-')]
        ### get include values
        include_values = [x for x in fval if not x.startswith('-')]
        ### process wildcards
        exclude_query.update(process_wildcards_int(exclude_values, key_base, include_flag=False))
        query.update(process_wildcards_int(include_values, key_base))
    return query, exclude_query


def build_query(GET_parameters):
    ### start the query parameters
    query = {}
    ### query for exclude
    exclude_query = {}
    ### start the schedconfig query parameters
    schedconfig_query = {}
    ### query for exclude in schedconfig
    schedconfig_exclude_query = {}

    ### configure time interval for queries
    starttime, endtime, nhours, errors_GET, f_computingsite, f_mcp_cloud, f_jobstatus, f_corecount, f_jobtype, \
        f_cloud, f_atlas_site, f_status = configure(GET_parameters)

    ### filter logdate__range
    query['modificationtime__castdate__range'] = [starttime, endtime]

    ### filter mcp_cloud
    mcp_cloud_query, mcp_cloud_exclude_query = parse_param_values_str(f_mcp_cloud, 'cloud')
    if len(mcp_cloud_query.keys()):
        query.update(mcp_cloud_query)
    if len(mcp_cloud_exclude_query.keys()):
        exclude_query.update(mcp_cloud_exclude_query)

    ### filter computingsite
    computingsite_query, computingsite_exclude_query = parse_param_values_str(f_computingsite, 'computingsite')
    if len(computingsite_query.keys()):
        query.update(computingsite_query)
    if len(computingsite_exclude_query.keys()):
        exclude_query.update(computingsite_exclude_query)

    ### filter jobstatus
    jobstatus_query, jobstatus_exclude_query = parse_param_values_str(f_jobstatus, 'jobstatus')
    if len(jobstatus_query.keys()):
        query.update(jobstatus_query)
    if len(jobstatus_exclude_query.keys()):
        exclude_query.update(jobstatus_exclude_query)

    ### filter corecount
    corecount_query, corecount_exclude_query = parse_param_values_int(f_corecount, 'corecount')
    if len(corecount_query.keys()):
        schedconfig_query.update(corecount_query)
    if len(corecount_exclude_query.keys()):
        schedconfig_exclude_query.update(corecount_exclude_query)

    ### filter jobtype
    # jobtype based on prodsourcelabel, so convert it first then build query
    f_jobtype = convert_jobtype_prodsourcelabel(f_jobtype)
    jobtype_query, jobtype_exclude_query = parse_param_values_str(f_jobtype, 'prodsourcelabel')
    if len(jobtype_query.keys()):
        query.update(jobtype_query)
    if len(jobtype_exclude_query.keys()):
        exclude_query.update(jobtype_exclude_query)

    ### filter cloud
    cloud_query, cloud_exclude_query = parse_param_values_str(f_cloud, 'cloud')
    if len(cloud_query.keys()):
        schedconfig_query.update(cloud_query)
    if len(cloud_exclude_query.keys()):
        schedconfig_exclude_query.update(cloud_exclude_query)

    ### filter atlas_site
    atlas_site_query, atlas_site_exclude_query = parse_param_values_str(f_atlas_site, 'atlas_site')
    if len(atlas_site_query.keys()):
        schedconfig_query.update(atlas_site_query)
    if len(atlas_site_exclude_query.keys()):
        schedconfig_exclude_query.update(atlas_site_exclude_query)

    ### filter status
    status_query, status_exclude_query = parse_param_values_str(f_status, 'status')
    if len(status_query.keys()):
        schedconfig_query.update(status_query)
    if len(status_exclude_query.keys()):
        schedconfig_exclude_query.update(status_exclude_query)

    return query, exclude_query, starttime, endtime, nhours, errors_GET, schedconfig_query, schedconfig_exclude_query


def convert_jobtype_prodsourcelabel(qval_str):
    conv_str = ''
    conv_dict = {
        'analysis': ['panda', 'user'],
        'production': ['managed'],
        'test': ['prod_test', 'ptest', 'install', 'rc_alrb', 'rc_test2'],
    }
    jtypes = qval_str.split(',')
    for jt in jtypes:
        if jt in conv_dict:
            conv_str += ','.join(conv_dict[jt]) + ','
    if conv_str.endswith(','):
        conv_str = conv_str[:-1]
    return conv_str


def get_topo_info():
    """
    Getting PanDA resources info
    """
    pqs = get_panda_queues()
    res = {}
    needed_params = ['cloud', 'siteid', 'atlas_site', 'site', 'corecount', 'status', 'comment']
    for pq_name, pq_info in pqs.items():
        res[pq_name] = {}
        for param in needed_params:
            if param in pq_info:
                res[pq_name][param] = pq_info[param]
            else:
                res[pq_name][param] = None

    return res


def sort_data_by_cloud(data):
    """
        sort_data_by_cloud
        
        data: list of dictionaries
                    one dictionary per PanDA schedresource (computingsite)
                    keys: 
                        cloud
                        computingsite
                        and a bunch of other keys by job status, see STATELIST
        
        returns: input data sorted by cloud, computingsite
    """
    res = sorted(data, key=lambda x: (str(x['cloud']).lower(), str(x['computingsite']).lower()))
    return res


def summarize_data(data, query, exclude_query, schedconfig_query, schedconfig_exclude_query):
    """
        summarize_data
        
        data: queryset, list of dictionaries, e.g. 
            [{'njobs': 1, 'computingsite': u'CERN-PROD', 'jobstatus': u'holding'}]
        
        returns: list of dictionaries
                    one dictionary per PanDA schedresource (computingsite)
                    keys: 
                        cloud
                        computingsite
                        and a bunch of other keys by job status, see STATELIST
    """
    result = []

    # Data comes from jobs tables, does not take into account sites related filters like atlas_site, panda_site etc.
    # Therefore let's get all the PanDA Queues and filter them in advance.
    # get all sites topo from schedconfig table
    schedinfo = get_topo_info()
    # filter
    for param in ['corecount', 'status', 'cloud', 'atlas_site', 'status']:
        # handle excludes
        if param + '__exact' in schedconfig_exclude_query:
            schedinfo = {k: v for k, v in schedinfo.items() if v[param] != schedconfig_exclude_query[param + '__exact']}
        if param + '__istartswith' in schedconfig_exclude_query:
            schedinfo = {k: v for k, v in schedinfo.items() if not v[param].startswith(schedconfig_exclude_query[param + '__istartswith'])}
        if param + '__iendswith' in schedconfig_exclude_query:
            schedinfo = {k: v for k, v in schedinfo.items() if not v[param].endswith(schedconfig_exclude_query[param + '__iendswith'])}
        if param + '__icontains' in schedconfig_exclude_query:
            schedinfo = {k: v for k, v in schedinfo.items() if schedconfig_exclude_query[param + '__icontains'] not in v[param]}
        if param + '__in' in schedconfig_exclude_query:
            schedinfo = {k: v for k, v in schedinfo.items() if v[param] not in schedconfig_exclude_query[param + '__in']}

        # handle includes
        if param + '__exact' in schedconfig_query:
            schedinfo = {k: v for k, v in schedinfo.items() if v[param] == schedconfig_query[param + '__exact']}
        if param + '__istartswith' in schedconfig_query:
            schedinfo = {k: v for k, v in schedinfo.items() if v[param].startswith(schedconfig_query[param + '__istartswith'])}
        if param + '__iendswith' in schedconfig_query:
            schedinfo = {k: v for k, v in schedinfo.items() if v[param].endswith(schedconfig_query[param + '__iendswith'])}
        if param + '__icontains' in schedconfig_query:
            schedinfo = {k: v for k, v in schedinfo.items() if schedconfig_query[param + '__icontains'] not in v[param]}
        if param + '__in' in schedconfig_query:
            schedinfo = {k: v for k, v in schedinfo.items() if v[param] in schedconfig_query[param + '__in']}

    # get list of computing sites
    computingsites = list(set([x['computingsite'] for x in data]))

    # loop through computing sites, sum njobs for each job status
    for computingsite in computingsites:
        if computingsite in schedinfo:
            item = {'computingsite': computingsite}
            item['cloud'] = schedinfo[computingsite]['cloud'] if 'cloud' in schedinfo[computingsite] else None
            item['atlas_site'] = schedinfo[computingsite]['atlas_site'] if 'atlas_site' in schedinfo[computingsite] else None
            item['corecount'] = schedinfo[computingsite]['corecount'] if 'corecount' in schedinfo[computingsite] else None
            item['status'] = schedinfo[computingsite]['status'] if 'status' in schedinfo[computingsite] else None
            item['comment'] = schedinfo[computingsite]['comment'] if 'comment' in schedinfo[computingsite] else None
            item['mcp_cloud'] = None  # legacy

            # get records for this computingsite
            rec = [x for x in data if x['computingsite'] == computingsite]

            # get njobs per jobstatus for this computingsite
            for jobstatus in const.JOB_STATES:
                process_jobstatus = False
                if 'jobstatus__in' in query.keys() and jobstatus in query['jobstatus__in']:
                    process_jobstatus = True
                elif 'jobstatus__in' not in query.keys():
                    process_jobstatus = True
                if process_jobstatus:
                    jobstatus_rec = [x['njobs'] for x in rec if x['jobstatus'] == jobstatus]
                    item[jobstatus] = sum(jobstatus_rec)
                else:
                    item[jobstatus] = None
            # store info for this computingsite
            result.append(item)

    # sort result
    result = sort_data_by_cloud(result)
    return result


