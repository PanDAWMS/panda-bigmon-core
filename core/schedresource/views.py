import json
import logging
from datetime import datetime, timedelta

from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render
from django.utils.cache import patch_response_headers

from core.common.utils import getContextVariables
from core.libs.DateEncoder import DateEncoder
from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.site import get_pq_metrics
from core.libs.sqlcustom import escape_input
from core.oauth.utils import login_customrequired
from core.schedresource.models import SchedconfigJson
from core.schedresource.utils import get_panda_queues, filter_pq_json, get_panda_resource, site_summary_dict
from core.utils import extensibleURL, removeParam, is_json_request
from core.views import initRequest

_logger = logging.getLogger('bigpandamon')


@login_customrequired
def siteList(request):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "siteList")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'siteList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = 'alpha'
    for param in request.session['requestParams']:
        request.session['requestParams'][param] = escape_input(request.session['requestParams'][param])

    # get full list of queues
    pqs = get_panda_queues()
    if 'copytool' in request.session['requestParams'] and request.session['requestParams']['copytool'] is not None:
        pqs = {k: v for k, v in pqs.items() if request.session['requestParams']['copytool'] in v['copytools']}

    pqs = filter_pq_json(request, pqs_dict=pqs)
    pqs = list(pqs.values())

    xurl = extensibleURL(request)
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    if not is_json_request(request):
        # prepare data for table
        for pq in pqs:
            if 'maxrss' in pq and isinstance(pq['maxrss'], int):
                pq['maxrss_gb'] = round(pq['maxrss'] / 1000., 1)
            if 'minrss' in pq and isinstance(pq['minrss'], int):
                pq['minrss_gb'] = round(pq['minrss'] / 1000., 1)
            if 'maxtime' in pq and isinstance(pq['maxtime'], int) and pq['maxtime'] > 0:
                pq['maxtime_hours'] = round(pq['maxtime'] / 3600.)
            if 'maxinputsize' in pq and isinstance(pq['maxinputsize'], int) and pq['maxinputsize'] > 0:
                pq['maxinputsize_gb'] = round(pq['maxinputsize'] / 1000.)
            if 'copytools' in pq and pq['copytools'] and len(pq['copytools']) > 0:
                pq['copytool'] = ', '.join(list(pq['copytools'].keys()))
            if 'queues' in pq and pq['queues'] and len(pq['queues']) > 0:
                pq['job_manager'] = ', '.join(list(set(
                    [q['ce_jobmanager'] for q in pq['queues'] if 'ce_jobmanager' in q and q['ce_jobmanager']]
                )))
            elif 'system' in pq and pq['system']:
                pq['job_manager'] = pq['system']
            else:
                pq['job_manager'] = ''

            # attribute summary
            sumd = site_summary_dict(pqs, vo=settings.MON_VO, sortby=sortby)
        pq_params_table = [
            'cloud', 'gocname', 'tier', 'nickname', 'status', 'type', 'workflow', 'job_manager', 'copytool', 'harvester',
            'minrss_gb', 'maxrss_gb', 'maxtime_hours', 'maxinputsize_gb', 'comment'
        ]
        sites = []
        for pq in pqs:
            tmp_dict = {}
            for param in pq_params_table:
                tmp_dict[param] = pq[param] if param in pq and pq[param] is not None else '---'
            sites.append(tmp_dict)

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'sites': sites,
            'sumd': sumd,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        setCacheEntry(request, "siteList", json.dumps(data, cls=DateEncoder), 60 * 60)
        response = render(request, 'siteList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(pqs, cls=DateEncoder), content_type='application/json')


@login_customrequired
def siteInfo(request, site=''):
    valid, response = initRequest(request)
    if not valid:
        return response

    if site == '' and 'site' in request.session['requestParams']:
        site = request.session['requestParams']['site']

    # get data from new schedconfig_json table
    HPC = False
    njobhours = 12
    panda_queue = []
    pq_dict = None
    pqquery = {'pandaqueue': site}
    panda_queues = SchedconfigJson.objects.filter(**pqquery).values()
    panda_queue_type = None

    if len(panda_queues) > 0:
        pq_dict = panda_queues[0]['data']
        if isinstance(pq_dict, str):
            pq_dict = json.loads(pq_dict)
    # get PQ params from CRIC if no info in DB
    if not pq_dict:
        pq_dict = get_panda_resource(site)

    if pq_dict:
        panda_queue_type = pq_dict['type']
        for par, val in pq_dict.items():
            val = ', '.join(
                [str(subpar) + ' = ' + str(subval) for subpar, subval in val.items()]
            ) if isinstance(val, dict) else val
            panda_queue.append({'param': par, 'value': val})
        panda_queue = sorted(panda_queue, key=lambda x: x['param'])

        # if HPC increase hours for links
        if 'catchall' in pq_dict and pq_dict['catchall'] and pq_dict['catchall'].find('HPC') >= 0:
            HPC = True
            njobhours = 48

    if not is_json_request(request):
        # prepare relevant params for top table
        attrs = []
        if pq_dict:
            attrs.append({'name': 'GOC name', 'value': pq_dict['gocname'] if 'gocname' in pq_dict else ''})
            if HPC:
                attrs.append({'name': 'HPC', 'value': 'This is a High Performance Computing (HPC) supercomputer queue'})
            if 'catchall' in pq_dict and pq_dict['catchall'].find('log_to_objectstore') >= 0:
                attrs.append({'name': 'Object store logs', 'value': 'Logging to object store is enabled'})
            if 'objectstore' in pq_dict and pq_dict['objectstore'] and len(pq_dict['objectstore']) > 0:
                fields = pq_dict['objectstore'].split('|')
                nfields = len(fields)
                for nf in range(0, len(fields)):
                    if nf == 0:
                        attrs.append({'name': 'Object store location', 'value': fields[0]})
                    else:
                        fields2 = fields[nf].split('^')
                        if len(fields2) > 1:
                            ostype = fields2[0]
                            ospath = fields2[1]
                            attrs.append({'name': 'Object store %s path' % ostype, 'value': ospath})

            if 'nickname' in pq_dict and pq_dict['nickname'] != site:
                attrs.append({'name': 'Queue (nickname)', 'value': pq_dict['nickname']})
            attrs.append({'name': 'Status', 'value': pq_dict['status'] if 'status' in pq_dict else '-'})
            if 'comment' in pq_dict and pq_dict['comment'] and len(pq_dict['comment']) > 0:
                attrs.append({'name': 'Comment', 'value': pq_dict['comment']})
            if 'type' in pq_dict and pq_dict['type']:
                attrs.append({'name': 'Type', 'value': pq_dict['type']})
            if 'cloud' in pq_dict and pq_dict['cloud']:
                attrs.append({'name': 'Cloud', 'value': pq_dict['cloud']})
            if 'tier' in pq_dict and pq_dict['tier']:
                attrs.append({'name': 'Tier', 'value': pq_dict['tier']})
            if 'corecount' in pq_dict and isinstance(pq_dict['corecount'], int):
                attrs.append({'name': 'Cores', 'value': pq_dict['corecount']})
            if 'maxrss' in pq_dict and isinstance(pq_dict['maxrss'], int):
                attrs.append({'name': 'Max RSS', 'value': "{} GB".format(round(pq_dict['maxrss'] / 1000., 1))})
            if 'maxtime' in pq_dict and isinstance(pq_dict['maxtime'], int) and pq_dict['maxtime'] > 0:
                attrs.append({'name': 'Max time', 'value': "{} hours".format(round(pq_dict['maxtime'] / 3600.))})
            if 'maxinputsize' in pq_dict and isinstance(pq_dict['maxinputsize'], int) and pq_dict['maxinputsize'] > 0:
                attrs.append(
                    {'name': 'Max input size', 'value': "{} GB".format(round(pq_dict['maxinputsize'] / 1000.))})

            # get calculated metrics
            if 'ATLAS' in settings.DEPLOYMENT:
                try:
                    metrics = get_pq_metrics(pq_dict['nickname'])
                except Exception as ex:
                    metrics = {}
                    _logger.exception('Failed to get metrics for {}\n {}'.format(pq_dict['nickname'], ex))
                if len(metrics) > 0:
                    for pq, m_dict in metrics.items():
                        for m in m_dict:
                            panda_queue.append({'label': m, 'param': m, 'value': m_dict[m]})

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'site': pq_dict,
            'colnames': panda_queue,
            'attrs': attrs,
            'name': site,
            'pq_type': panda_queue_type,
            'njobhours': njobhours,
            'hc_link_dates': [
                (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d")],
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render(request, 'siteInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']

        return HttpResponse(json.dumps(panda_queue), content_type='application/json')
