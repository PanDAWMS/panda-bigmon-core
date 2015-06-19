"""
    status_summary.utils
    
"""
import logging
import pytz
from datetime import datetime, timedelta

from django.db.models import Count, Sum

from ..resource.models import Schedconfig
from core.views import statelist as STATELIST


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
                'Provided starttime [%s] has incorrect format, expected [%s].' % \
                (starttime, defaultDatetimeFormat)
            starttime = datetime.utcnow() - timedelta(hours=nhours)
            starttime = starttime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        ### endtime
        endtime = request_GET['endtime']
        try:
            dt_end = datetime.strptime(endtime, defaultDatetimeFormat)
        except ValueError:
            errors_GET['endtime'] = \
                'Provided endtime [%s] has incorrect format, expected [%s].' % \
                (endtime, defaultDatetimeFormat)
            endtime = datetime.utcnow()
            endtime = endtime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    ### if nhours is provided, do query "last N days"
    elif 'nhours' in request_GET:
        try:
            nhours = int(request_GET['nhours'])
        except:
            nhours = 12
            errors_GET['nhours'] = \
                'Wrong or no nhours has been provided.Using [%s].' % \
                (nhours)
        starttime = datetime.utcnow() - timedelta(hours=nhours)
        starttime = starttime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        endtime = datetime.utcnow()
        endtime = endtime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    ### neither nhours, nor starttime&endtime was provided
    else:
        nhours = 12
        starttime = datetime.utcnow() - timedelta(hours=nhours)
        starttime = starttime.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        endtime = datetime.utcnow()
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

    return starttime, endtime, nhours, errors_GET, \
        f_computingsite, f_mcp_cloud, f_jobstatus, f_corecount, f_cloud, \
        f_atlas_site, f_status


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

    ### filter mcp_cloud
    fval = GET_param_field.split(',')
    if len(fval) and len(fval[0]):
        ### get exclude values
        exclude_values = [x[1:] for x in fval \
                          if x.startswith('-')]
        ### get include values
        include_values = [x for x in fval \
                          if not x.startswith('-')]
        ### process wildcards
        exclude_query.update(process_wildcards_str(exclude_values, \
                                                key_base, include_flag=False))
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
        exclude_values = [x[1:] for x in fval \
                          if x.startswith('-')]
        ### get include values
        include_values = [x for x in fval \
                          if not x.startswith('-')]
        ### process wildcards
        exclude_query.update(process_wildcards_int(exclude_values, \
                                                key_base, include_flag=False))
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
    starttime, endtime, nhours, errors_GET, \
        f_computingsite, f_mcp_cloud, f_jobstatus, f_corecount, f_cloud, \
        f_atlas_site, f_status = configure(GET_parameters)

    ### filter logdate__range
    query['modificationtime__range'] = [starttime, endtime]

    ### filter mcp_cloud
    mcp_cloud_query, mcp_cloud_exclude_query = \
        parse_param_values_str(f_mcp_cloud, 'cloud')
    if len(mcp_cloud_query.keys()):
        query.update(mcp_cloud_query)
    if len(mcp_cloud_exclude_query.keys()):
        exclude_query.update(mcp_cloud_exclude_query)

    ### filter computingsite
    computingsite_query, computingsite_exclude_query = \
        parse_param_values_str(f_computingsite, 'computingsite')
    if len(computingsite_query.keys()):
        query.update(computingsite_query)
    if len(computingsite_exclude_query.keys()):
        exclude_query.update(computingsite_exclude_query)

    ### filter jobstatus
    jobstatus_query, jobstatus_exclude_query = \
        parse_param_values_str(f_jobstatus, 'jobstatus')
    if len(jobstatus_query.keys()):
        query.update(jobstatus_query)
    if len(jobstatus_exclude_query.keys()):
        exclude_query.update(jobstatus_exclude_query)

    ### filter corecount
    corecount_query, corecount_exclude_query = \
        parse_param_values_int(f_corecount, 'corecount')
    if len(corecount_query.keys()):
        schedconfig_query.update(corecount_query)
    if len(corecount_exclude_query.keys()):
        schedconfig_exclude_query.update(corecount_exclude_query)

    ### filter cloud
    cloud_query, cloud_exclude_query = \
        parse_param_values_str(f_cloud, 'cloud')
    if len(cloud_query.keys()):
        schedconfig_query.update(cloud_query)
    if len(cloud_exclude_query.keys()):
        schedconfig_exclude_query.update(cloud_exclude_query)

    ### filter atlas_site
    atlas_site_query, atlas_site_exclude_query = \
        parse_param_values_str(f_atlas_site, 'atlas_site')
    if len(atlas_site_query.keys()):
        schedconfig_query.update(atlas_site_query)
    if len(atlas_site_exclude_query.keys()):
        schedconfig_exclude_query.update(atlas_site_exclude_query)

    ### filter status
    status_query, status_exclude_query = \
        parse_param_values_str(f_status, 'status')
    if len(status_query.keys()):
        schedconfig_query.update(status_query)
    if len(status_exclude_query.keys()):
        schedconfig_exclude_query.update(status_exclude_query)

    return query, exclude_query, starttime, endtime, nhours, errors_GET, \
        schedconfig_query, schedconfig_exclude_query


def get_topo_info():
    res = {}
    schedinfo = Schedconfig.objects.all().values('cloud', 'siteid', 'gstat', \
                            'site', 'corecount', 'status', 'comment_field')
    res = dict([(x['siteid'], x) for x in schedinfo])
    return res


def sort_data_by_cloud(data):
    """
        sort_data_by_cloud
        
        data: list of dictionaries
                    one dictionary per PanDA resource (computingsite)
                    keys: 
                        cloud
                        computingsite
                        and a bunch of other keys by job status, see STATELIST
        
        returns: input data sorted by cloud, computingsite
    """
    res = sorted(data, key=lambda x: (str(x['cloud']).lower(), \
                                      str(x['computingsite']).lower()))
    return res


def summarize_data(data, query, exclude_query, schedconfig_query, \
                             schedconfig_exclude_query):
    """
        summarize_data
        
        data: queryset, list of dictionaries, e.g. 
            [{'njobs': 1, 'computingsite': u'CERN-PROD', 'jobstatus': u'holding'}]
        
        returns: list of dictionaries
                    one dictionary per PanDA resource (computingsite)
                    keys: 
                        cloud
                        computingsite
                        and a bunch of other keys by job status, see STATELIST
    """
    result = []
    ### get all sites topo from schedconfig table
    schedinfo = get_topo_info()
    ### get list of computing sites
    computingsites = list(set([x['computingsite'] for x in data]))
    ### loop through computing sites, sum njobs for each job status
    for computingsite in computingsites:
        ### data comes from jobs tables, does not take into account
        ###   schedconfig related filters. Therefore let's use flag store
        ###   and set flag store=False when schedconfig filter excludes this
        ###   computingsite record.
        store = True
        item={'computingsite': computingsite}
        ### add topology info
        cloud = None
        atlas_site = None
        corecount = None
        status = None
        comment = None
        if computingsite in schedinfo:
            cs_schedinfo = schedinfo[computingsite]
            cloud = cs_schedinfo['cloud']
            atlas_site = cs_schedinfo['gstat']
            corecount = cs_schedinfo['corecount']
            status = cs_schedinfo['status']
            comment = cs_schedinfo['comment_field']
        item['cloud'] = cloud
        item['atlas_site'] = atlas_site
        item['corecount'] = corecount
        item['status'] = status
        item['comment'] = comment
        print
        print '348 query', query
        print '348 exclude_query', exclude_query
        print '348 schedconfig_query', schedconfig_query
        print '348 schedconfig_exclude_query', schedconfig_exclude_query
        for schedconfig_key_base in ['corecount', 'status', 'comment', \
                                     'cloud', 'atlas_site', 'status']:
            ### handle excludes
            if '%s__isnull' % (schedconfig_key_base) in \
            schedconfig_exclude_query.keys():
                if str(item[schedconfig_key_base]).upper() != 'NULL':
                    store = False
            if '%s__exact' % (schedconfig_key_base) in \
            schedconfig_exclude_query.keys():
                if str(schedconfig_exclude_query['%s__exact' % (schedconfig_key_base)]) == \
                    str(item[schedconfig_key_base]):
                    store = False
            if '%s__in' % (schedconfig_key_base) in \
            schedconfig_exclude_query.keys():
                for sch_it in schedconfig_exclude_query['%s__in' % (schedconfig_key_base)]:
                    if str(sch_it).upper() == str(item[schedconfig_key_base]).upper():
                        store = False
            if '%s__istartswith' % (schedconfig_key_base) in \
            schedconfig_exclude_query.keys():
                if str(item[schedconfig_key_base]).upper().find(\
                        str(schedconfig_exclude_query['%s__istartswith' % \
                                (schedconfig_key_base)]).upper()) == 0:
                    store = False
            if '%s__iendswith' % (schedconfig_key_base) in \
            schedconfig_exclude_query.keys():
                if str(item[schedconfig_key_base]).upper().find(\
                        str(schedconfig_exclude_query['%s__iendswith' % \
                                (schedconfig_key_base)]).upper()) == \
                                len(item[schedconfig_key_base]) - len(\
                                schedconfig_exclude_query['%s__iendswith' % \
                                (schedconfig_key_base)]):
                    store = False
            if '%s__icontains' % (schedconfig_key_base) in \
            schedconfig_exclude_query.keys():
                if str(item[schedconfig_key_base]).upper().find(\
                        str(schedconfig_exclude_query['%s__icontains' % \
                                (schedconfig_key_base)]).upper()) == -1:
                    store = False
            ### handle includes
            if '%s__isnull' % (schedconfig_key_base) in \
            schedconfig_query.keys():
                if str(item[schedconfig_key_base]).upper() != 'NULL':
                        store = False
            if '%s__exact' % (schedconfig_key_base) in \
            schedconfig_query.keys():
                if str(schedconfig_query['%s__exact' % (schedconfig_key_base)]) != \
                    str(item[schedconfig_key_base]):
                    store = False
            if '%s__in' % (schedconfig_key_base) in \
            schedconfig_query.keys():
                for sch_it in schedconfig_query['%s__in' % (schedconfig_key_base)]:
                    if str(sch_it).upper() != str(item[schedconfig_key_base]).upper():
                        store = False
            if '%s__istartswith' % (schedconfig_key_base) in \
            schedconfig_query.keys():
                print '401', '%s__istartswith' % (schedconfig_key_base)
                if str(item[schedconfig_key_base]).upper().find(\
                            str(schedconfig_query['%s__istartswith' % \
                                (schedconfig_key_base)]).upper()) != 0:
                    print '401', store
                    store = False
                    print '401', store
            if '%s__iendswith' % (schedconfig_key_base) in \
            schedconfig_query.keys():
                for sch_it in schedconfig_query['%s__iendswith' % (schedconfig_key_base)]:
                    if str(item[schedconfig_key_base]).upper().find(\
                                str(sch_it).upper()) != \
                                len(item[schedconfig_key_base]) - len(sch_it):
                        store = False
            if '%s__icontains' % (schedconfig_key_base) in \
            schedconfig_query.keys():
                if str(item[schedconfig_key_base]).upper().find(\
                                str(schedconfig_query['%s__icontains' % \
                                (schedconfig_key_base)]).upper()) == -1:
                    store = False
        if store:
            ### get records for this computingsite
            rec = [x for x in data \
                   if x['computingsite'] == computingsite]
            ### get cloud for this computingsite
            mcp_cloud = ', '.join(sorted(list(set([x['cloud'] for x in data \
                     if x['computingsite'] == computingsite and len(x['cloud']) > 1]))))
            item['mcp_cloud'] = mcp_cloud
            ### get njobs per jobstatus for this computingsite
            for jobstatus in STATELIST:
                process_jobstatus = False
                if 'jobstatus__in' in query.keys() and jobstatus in query['jobstatus__in']:
                    process_jobstatus = True
                elif 'jobstatus__in' not in query.keys():
                    process_jobstatus = True
                if process_jobstatus:
                    jobstatus_rec = [x['njobs'] for x in rec \
                                 if x['jobstatus'] == jobstatus]
                    item[jobstatus] = sum(jobstatus_rec)
                else:
                    item[jobstatus] = None
            ### store info for this computingsite
            result.append(item)
    ### sort result
    result = sort_data_by_cloud(result)
    return result


