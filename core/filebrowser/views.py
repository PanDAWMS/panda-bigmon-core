"""
    filebrowser.views
    
"""
import logging
import re
import json
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.template.loader import get_template
from django.conf import settings
from .utils import get_rucio_file, get_rucio_pfns_from_guids, fetch_file, get_filebrowser_vo, \
    remove_folder, get_fullpath_filebrowser_directory, list_file_directory

from core.common.models import Filestable4, FilestableArch
from core.views import DateTimeEncoder, initSelfMonitor
from datetime import datetime

_logger = logging.getLogger('bigpandamon-filebrowser')
filebrowserDateTimeFormat = "%Y %b %d %H:%M:%S"
hostname = "bigpanda.cern.ch"

def index(request):
    """
        index -- filebrowser's default page
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
    """

    try:
        initSelfMonitor(request)
    except:
        _logger.exception('Failed to init self monitor')

    errors = {}

    _logger.debug("index started - " + datetime.now().strftime("%H:%M:%S") + "  ")

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
    pattern_site = '^[a-zA-Z0-9.,\-_\/]+$'
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

    # check if size of logfile is too big return to user error message containing rucio cli command to download it locally
    max_sizemb = 1000
    sizemb = None
    try:
        fileid = int(request.GET['fileid'])
    except:
        fileid = -1
    lquery = {'type': 'log'}
    if lfn and len(lfn) > 0:
        lquery['lfn'] = lfn
        fsize = Filestable4.objects.filter(**lquery).values('fsize', 'fileid')
        if len(fsize) == 0:
            fsize = FilestableArch.objects.filter(**lquery).values('fsize', 'fileid')
        if len(fsize) > 0:
            try:
                if fileid > 0:
                    sizemb = round(int([f['fsize'] for f in fsize if f['fileid'] == fileid][0])/1000/1000)
                else:
                    sizemb = round(int([f['fsize'] for f in fsize][0])/1000/1000)
            except:
                _logger.warning("ERROR!!! Failed to calculate log tarball size in MB")

    _logger.debug("index step1 - " + datetime.now().strftime("%H:%M:%S") + "  ")

    ### download the file
    files = []
    dirprefix = ''
    tardir = ''
    if sizemb and sizemb > max_sizemb:
        _logger.warning('Size of the requested log is {} MB which is more than limit {} MB'.format(sizemb, max_sizemb))
        errormessage = """The size of requested log is too big ({}MB). 
                            Please try to download it locally using Rucio CLI by the next command: 
                            rucio download {}:{}""".format(sizemb, scope, lfn)
        data = {
            'errormessage': errormessage
        }
        return render_to_response('errorPage.html', data, content_type='text/html')
    if not (guid is None or lfn is None or scope is None):
        files, errtxt, dirprefix, tardir = get_rucio_file(scope,lfn, guid, 100)
    else:
        errormessage = ''
        if guid is None:
            errormessage = 'No guid provided.'
        elif lfn is None:
            errormessage = 'No lfn provided.'
        elif scope is None:
            errormessage = 'No scope provided.'
        _logger.warning(errormessage)
        data = {
            'errormessage': errormessage
        }
        return render_to_response('errorPage.html', data, content_type='text/html')
    if not len(files):
        msg = 'Something went wrong while the log file downloading. [guid=%s, site=%s, scope=%s, lfn=%s] \n' % \
              (guid, site, scope, lfn)
        _logger.warning(msg)
        errors['download'] = msg
    if len(errtxt):
        if 'download' not in errors:
            errors['download'] = ''
        errors['download'] += errtxt

    _logger.debug("index step2 - " + datetime.now().strftime("%H:%M:%S") + "  ")

    totalLogSize = 0
    if type(files) is list and len(files) > 0:
        for file in files:
            totalLogSize += file['size'] if 'size' in file and file['size'] > 0 else 0

    # from B to MB
    if totalLogSize > 0:
        totalLogSize = round(totalLogSize*1.0/1024/1024, 2)

    ### return the file page


    ### set request response data
    data = {
        'request': request,
        'errors': errors,
        'pfns': pfns,
        'files': files,
        'dirprefix': dirprefix,
        'tardir': tardir,
        'scope': scope,
        'lfn': lfn,
        'site': site,
        'guid': guid,
        'MEDIA_URL': settings.MEDIA_URL,
        'viewParams' : {'MON_VO': str(get_filebrowser_vo()).upper()},
        'HOSTNAME': hostname,
        'totalLogSize': totalLogSize,
        'nfiles': len(files),
    }

    _logger.debug("index step3 - " + datetime.now().strftime("%H:%M:%S") + "  ")
    if 'json' not in request.GET:
        return render_to_response('filebrowser/filebrowser_index.html', data, RequestContext(request))
    else:
        resp = HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
        _logger.debug("index step4 - " + datetime.now().strftime("%H:%M:%S") + "  ")
        return resp



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
        url = 'http://' + hostname + \
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


def get_job_memory_monitor_output(pandaid):
    """
    Download log tarball of a job and return path to a local copy of memory_monitor_output.txt file
    :param pandaid:
    :return: mmo_path: str
    """
    mmo_path = None
    files = []
    scope = ''
    lfn = ''
    guid = ''
    dirprefix = ''
    tardir = ''
    query = {}
    query['type'] = 'log'
    query['pandaid'] = int(pandaid)
    values = ['pandaid', 'guid', 'scope', 'lfn']
    file_properties = []

    file_properties.extend(Filestable4.objects.filter(**query).values(*values))
    if len(file_properties) == 0:
        file_properties.extend(FilestableArch.objects.filter(**query).values(*values))

    if len(file_properties):
        file_properties = file_properties[0]
        try:
            guid = file_properties['guid']
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

        if guid and lfn and scope:
            # check if files are already available in common CEPH storage
            tarball_path = get_fullpath_filebrowser_directory() + '/' + guid.lower() + '/' + scope + '/' + lfn
            files, err, tardir = list_file_directory(tarball_path, 100)
            _logger.debug('tarball path is {} \nError message is {} \nGot tardir: {}'.format(tarball_path, err, tardir))
            if len(files) == 0 and len(err) > 0:
                # download tarball
                _logger.debug('log tarball has not been downloaded, so downloading it now')
                files, errtxt, dirprefix, tardir = get_rucio_file(scope, lfn, guid)
                _logger.debug('Got files for dir: {} and tardir: {}. Error message: {}'.format(dirprefix, tardir, errtxt))
            if type(files) is list and len(files) > 0:
                for f in files:
                    if f['name'] == 'memory_monitor_output.txt':
                        mmo_path = tarball_path + '/' + tardir + '/' + 'memory_monitor_output.txt'
    _logger.debug('Final mmo_path: {}'.format(mmo_path))
    return mmo_path


def delete_files(request):
    """
    Clear subfolder containing log files
    :param request:
    :return:
    """

    ### check that path to logs is provided
    guid = None
    try:
        guid = request.GET['guid']
    except:
        msg = 'Missing guid GET parameter'
        _logger.error(msg)

    ### clean folder if guid provided
    if guid is not None:
        logdir = remove_folder(guid)
        data = {'message':'The folder was cleaned ' + logdir}
        return HttpResponse(json.dumps(data), content_type='application/json')
    else:
        return HttpResponse(status=404)

