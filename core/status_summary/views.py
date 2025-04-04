""" 
core.status_summary.views

"""

import logging
import json
from datetime import datetime, timezone

from django.db.models import Count, Sum, Q
from django.shortcuts import render
from django.template import RequestContext, loader
from django.http import HttpResponse

from .utils import build_query, summarize_data
from core.pandajob.models import Jobsactive4, Jobsdefined4, Jobsarchived4

from core.views import initRequest
from core.oauth.utils import login_customrequired

collectorDatetimeFormat = "%Y-%m-%dT%H:%M:%S"
collectorTimeFormat = "%Y-%m-%d %H:%M:%S"

_logger = logging.getLogger('bigpandamon')


def index_data(request):
    """
        index -- status_summary's default page
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
        
        filtering options for specified GET parameters:
            * filtering on jobs properties
                ?nhours ... date range of how many hours in past
                ?starttime ... datetime from, format %Y-%m-%dT%H:%M:%S
                ?endtime ... datetime to, format %Y-%m-%dT%H:%M:%S
            
                nhours has higher priority than starttime, endtime
                    if nhours is specified, starttime&endtime are not taken into account.

                ?computingsite ... computingsite field of the jobs tables
                ?jobstatus ... PanDA job status, list delimited by comma
                ?jobtype ... PanDA job type: production, analysis or test
            
            * filtering on PanDA schedresource properties
                ?corecount .. corecount fieldfrom the CRIC
                ?cloud .. cloud field from the CRIC
                ?atlas_site .. gstat field from the CRIC
                ?status .. status field from the CRIC
    """
    errors = {}
    warnings = {}

    ### GET parameters
    GET_parameters = {}
    for p in request.GET:
        GET_parameters[p] = str(request.GET[p])

    ### time range from request.GET
    optionalFields = [
        'nhours', 'starttime', 'endtime',
        'computingsite', 'jobstatus', 'corecount', 'jobtype', 'cloud', 'atlas_site', 'status']
    for optionalField in optionalFields:
        try:
            if len(request.GET[optionalField]) < 1:
                msg = 'Missing optional GET parameter %s. ' % optionalField
                if 'missingoptionalparameter' not in warnings.keys():
                    warnings['missingoptionalparameter'] = ''
                warnings['missingoptionalparameter'] += msg
        except:
            msg = 'Missing optional GET parameter %s. ' % optionalField
            _logger.warning(msg)
            if 'missingoptionalparameter' not in warnings.keys():
                warnings['missingoptionalparameter'] = ''
            warnings['missingoptionalparameter'] += msg

    ### if all expected GET parameters are present, execute log lookup

    ### get queries
    query, exclude_query, starttime, endtime, nhours, errors_GET, schedconfig_query, schedconfig_exclude_query = build_query(GET_parameters)

    ### query jobs for the summary
    qs = []
    if len(exclude_query.keys()):
        qs_tmp = Jobsactive4.objects.filter(**query).filter(~Q(**exclude_query)
        ).values('jobstatus', 'cloud', 'computingsite'
        ).annotate(njobs=Count('jobstatus')
        ).order_by('cloud', 'computingsite', 'jobstatus')
        qs.extend(qs_tmp)

        qs_tmp = Jobsdefined4.objects.filter(**query).filter(~Q(**exclude_query)
        ).values('jobstatus', 'cloud', 'computingsite'
        ).annotate(njobs=Count('jobstatus')
        ).order_by('cloud', 'computingsite', 'jobstatus')
        qs.extend(qs_tmp)

        qs_tmp = Jobsarchived4.objects.filter(**query).filter(~Q(**exclude_query)
        ).values('jobstatus', 'cloud', 'computingsite'
        ).annotate(njobs=Count('jobstatus')
        ).order_by('cloud', 'computingsite', 'jobstatus')
        qs.extend(qs_tmp)
    else:
        qs_tmp = Jobsactive4.objects.filter(**query).values('jobstatus', 'cloud', 'computingsite'
        ).annotate(njobs=Count('jobstatus')
        ).order_by('cloud', 'computingsite', 'jobstatus')
        qs.extend(qs_tmp)
    
        qs_tmp = Jobsdefined4.objects.filter(**query).values('jobstatus', 'cloud', 'computingsite'
        ).annotate(njobs=Count('jobstatus')
        ).order_by('cloud', 'computingsite', 'jobstatus')
        qs.extend(qs_tmp)

        qs_tmp = Jobsarchived4.objects.filter(**query).values('jobstatus', 'cloud', 'computingsite'
        ).annotate(njobs=Count('jobstatus')
        ).order_by('cloud', 'computingsite', 'jobstatus')
        qs.extend(qs_tmp)

    if not len(qs):
        errors['lookup'] = "Job for this query has not been found. "

    qs_tidy = summarize_data(qs, query, exclude_query, schedconfig_query, schedconfig_exclude_query)
    ### merge queries
    query_merge = {}
    if len(query.keys()):
        for key in query.keys():
            new_key = 'JOBS INCLUDE: %s' % (key)
            query_merge[new_key] = query[key]
    if len(exclude_query.keys()):
        for key in exclude_query.keys():
            new_key = 'JOBS EXCLUDE: %s' % (key)
            query_merge[new_key] = exclude_query[key]
    if len(schedconfig_query.keys()):
        for key in schedconfig_query.keys():
            new_key = 'TOPO INCLUDE: %s' % (key)
            query_merge[new_key] = schedconfig_query[key]
    if len(schedconfig_exclude_query.keys()):
        for key in schedconfig_exclude_query.keys():
            new_key = 'TOPO EXCLUDE: %s' % (key)
            query_merge[new_key] = schedconfig_exclude_query[key]

    ### set request response data
    data = {
        'errors_GET': errors_GET,
        'starttime': starttime,
        'endtime': endtime,
        'nhours': nhours,
        'query': query_merge,
        'GETparams': GET_parameters,
        'data': qs_tidy,
    }
    return data, errors, warnings, query_merge, GET_parameters


def api_status_summary(request):
    """
        api_status_summary -- api for status_summary's default page
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
        for filtering options see index_data
    """
    raw_data_dict, errors, warnings, query, GET_parameters = index_data(request)
    raw_data = raw_data_dict['data']
    data = {
        'timestamp': datetime.now(tz=timezone.utc).isoformat(),
        'errors': errors,
        'warnings': warnings,
        'query': query,
        'GET_parameters': GET_parameters,
        'nrecords': len(raw_data),
        'data': raw_data,
    }

    if not len(errors) and len(raw_data):
        ### set request response data
        return  HttpResponse(json.dumps(data), content_type='application/json')
    elif not len(raw_data):
        return  HttpResponse(json.dumps(data), content_type='application/json', status=404)
    else:
        return  HttpResponse(json.dumps(data), content_type='application/json', status=400)


@login_customrequired
def index(request):
    """
        per_computingsite -- status_summary's default page
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
        for filtering options see index_data
    """
    valid, response = initRequest(request)
    if not valid:
        return response
    ### if curling for json, return API response
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return api_status_summary(request)

    data, errors, warnings, query, GET_parameters = index_data(request)
    data['viewParams'] = {'MON_VO': 'ATLAS'},
    data['request'] = request
    return render(request, 'per_computingsite.html', data, RequestContext(request))

