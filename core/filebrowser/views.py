"""
    filebrowser.views
    
"""
import logging
import re
from datetime import datetime
from django.http import HttpResponse
from django.shortcuts import render_to_response, render
from django.template import RequestContext, loader
from django.template.loader import get_template
from django.conf import settings
#from django.core.urlresolvers import reverse
#from django.views.decorators.csrf import ensure_csrf_cookie, csrf_exempt
from .utils import get_rucio_pfns_from_guids, fetch_file, get_filebrowser_vo, \
get_filebrowser_hostname

from core.common.models import Filestable4


_logger = logging.getLogger('bigpandamon-filebrowser')


def index(request):
    """
        index -- filebrowser's default page
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
    """
    errors = {}

    ### check that all expected parameters are in URL
    expectedFields = ['guid', 'site', 'scope', 'lfn']
    for expectedField in expectedFields:
        try:
            request.GET[expectedField]
        except:
            msg = 'Missing expected GET parameter %s. ' % expectedField
            _logger.error(msg)
            if 'missingparameter' not in errors.keys():
                errors['missingparameter'] = ''
            errors['missingparameter'] += msg

    ### if all expected GET parameters are present, execute file lookup
    pfns = []
    scope = ''
    lfn = ''
    guid = ''
    site = ''
    pattern_string='^[a-zA-Z0-9.\-_]+$'
    pattern_site = '^[a-zA-Z0-9.,\-_]+$'
    pattern_guid='^(\{){0,1}[0-9a-zA-Z]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\}){0,1}$'
    try:
        guid = request.GET['guid']
        if re.match(pattern_guid, guid) is None:
            guid = None
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += 'guid: %s ' % (request.GET['guid'])
    except:
        pass
    try:
        site = request.GET['site']
        if re.match(pattern_site, site) is None:
            site = None
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += 'site: %s ' % (request.GET['site'])
    except:
        pass
    try:
        lfn = request.GET['lfn']
        if re.match(pattern_string, lfn) is None:
            lfn = None
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += 'lfn: %s ' % (request.GET['lfn'])
    except:
        pass
    try:
        scope = request.GET['scope']
        if re.match(pattern_string, scope) is None:
            scope = None
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += 'scope: %s ' % (request.GET['scope'])
    except:
        pass

    if 'missingparameter' not in errors.keys() and \
       'improperformat' not in errors.keys():
        pfns, errtxt = get_rucio_pfns_from_guids(guids=[guid], site=[site], \
                    lfns=[lfn], scopes=[scope])
        if len(errtxt):
            if 'lookup' not in errors:
                errors['lookup'] = ''
            errors['lookup'] += errtxt

    ### download the file
    files = []
    dirprefix = ''
    tardir = ''
    if len(pfns):
        pfn = pfns[0]
        files, errtxt, dirprefix, tardir = fetch_file(pfn, guid)
        if not len(pfns):
            msg = 'File download failed. [pfn=%s guid=%s, site=%s, scope=%s, lfn=%s]' % \
                (pfn, guid, site, scope, lfn)
            _logger.warning(msg)
            errors['download'] = msg
        if len(errtxt):
            if 'download' not in errors:
                errors['download'] = ''
            errors['download'] += errtxt

    ### return the file page


    ### set request response data
    data = { \
        'errors': errors, \
        'pfns': pfns, \
        'files': files, \
        'dirprefix': dirprefix, \
        'tardir': tardir, \
        'scope': scope, \
        'lfn': lfn, \
        'site': site, \
        'guid': guid, \
        'viewParams' : {'MON_VO': str(get_filebrowser_vo()).upper()}, \
        'HOSTNAME': get_filebrowser_hostname() \
#        , 'new_contents': new_contents
    }
    return render_to_response('filebrowser/filebrowser_index.html', data, RequestContext(request))


def api_single_pandaid(request):
    """
        api_single_pandaid -- return log file URL for a single PanDA job
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
    """
    errors = {}

    ### check that all expected parameters are in URL
#    expectedFields = ['guid', 'site', 'scope', 'lfn']
    expectedFields = ['pandaid']
    for expectedField in expectedFields:
        try:
            if len(request.GET[expectedField]) < 1:
                msg = 'Missing expected GET parameter %s. ' % expectedField
                if 'missingparameter' not in errors.keys():
                    errors['missingparameter'] = ''
                errors['missingparameter'] += msg
        except:
            msg = 'Missing expected GET parameter %s. ' % expectedField
            _logger.error(msg)
            if 'missingparameter' not in errors.keys():
                errors['missingparameter'] = ''
            errors['missingparameter'] += msg

    ### if all expected GET parameters are present, execute file lookup
    pfns = []
    scope = ''
    lfn = ''
    guid = ''
    site = ''
    pandaid = None
    status = ''
    query = {}
    query['type'] = 'log'
    try:
        pandaid = int(request.GET['pandaid'])
    except:
        pass
    query['pandaid'] = pandaid
    file_properties = []
    try:
        file_properties = Filestable4.objects.filter(**query).values('pandaid', 'guid', \
                                'scope', 'lfn', 'destinationse', 'status')
    except:
        pass

    if len(file_properties):
        file_properties = file_properties[0]
        try:
            guid = file_properties['guid']
        except:
            pass
        try:
            site = file_properties['destinationse']
        except:
            pass
        try:
            lfn = file_properties['lfn']
        except:
            pass
        try:
            scope = file_properties['scope']
        except:
            pass
        try:
            status = file_properties['status']
        except:
            pass

        if 'missingparameter' not in errors.keys():
            pfns, errtxt = get_rucio_pfns_from_guids(guids=[guid], site=[site], \
                        lfns=[lfn], scopes=[scope])
            if len(errtxt):
                if 'lookup' not in errors:
                    errors['lookup'] = ''
                errors['lookup'] += errtxt
    
        ### download the file
        files = []
        dirprefix = ''
        tardir = ''
        if len(pfns):
            pfn = pfns[0]
            files, errtxt, dirprefix, tardir = fetch_file(pfn, guid, unpack=False, listfiles=False)
            if not len(pfns):
                msg = 'File download failed. [pfn=%s guid=%s, site=%s, scope=%s, lfn=%s]' % \
                    (pfn, guid, site, scope, lfn)
                _logger.warning(msg)
                errors['download'] = msg
            if len(errtxt):
                if 'download' in errors:
                    errors['download'] += errtxt
    else: # file not found in DB
        if 'lookup' not in errors:
            errors['lookup'] = ''
        errors['lookup'] += 'Log file for this job has not been found. '
    ### return the file page

    url = None
    data = { \
        'pandaid': pandaid, \
        'url': url, \
        'errors': errors, \
        'pfns': pfns, \
        'scope': scope, \
        'lfn': lfn, \
        'site': site, \
        'guid': guid, \
        'status': status, \
        'timestamp': datetime.utcnow().isoformat() \
    }
    if not len(errors):
        url = 'http://' + get_filebrowser_hostname() + \
                    settings.MEDIA_URL + dirprefix + '/' + lfn
        data['url'] = url
        ### set request response data
        return render_to_response('filebrowser/filebrowser_api_single_pandaid.html', {'data': data}, RequestContext(request))
    elif 'pandaid' not in request.GET.keys() or pandaid == None:
        t = get_template('filebrowser/filebrowser_api_single_pandaid.html')
        context = RequestContext(request, {'data':data})
        return HttpResponse(t.render(context), status=400)
    elif not len(file_properties):
        t = get_template('filebrowser/filebrowser_api_single_pandaid.html')
        context = RequestContext(request, {'data':data})
        return HttpResponse(t.render(context), status=404)
    else:
        t = get_template('filebrowser/filebrowser_api_single_pandaid.html')
        context = RequestContext(request, {'data':data})
        return HttpResponse(t.render(context), status=400)


