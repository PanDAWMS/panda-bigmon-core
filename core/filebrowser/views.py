"""
    filebrowser.views
    
"""
import logging
import re
import json
from django.http import HttpResponse
from django.shortcuts import render
from django.template import RequestContext
from django.conf import settings
from .utils import get_rucio_file, get_filebrowser_vo, remove_folder, get_fullpath_filebrowser_directory, \
    list_file_directory

from core.oauth.utils import login_customrequired
from core.common.models import Filestable4, FilestableArch
from core.views import initSelfMonitor, initRequest
from core.libs.job import get_job_list
from core.libs.DateTimeEncoder import DateTimeEncoder
from datetime import datetime

_logger = logging.getLogger('bigpandamon-filebrowser')
filebrowserDateTimeFormat = "%Y %b %d %H:%M:%S"
hostname = "bigpanda.cern.ch"


@login_customrequired
def index(request):
    """
        index -- filebrowser's default page
        
        :param request: Django's HTTP request 
        :type request: django.http.HttpRequest
        
    """
    valid, response = initRequest(request)
    # try:
    #     initSelfMonitor(request)
    # except:
    #     _logger.exception('Failed to init self monitor')

    errors = {}
    scope = None
    lfn = None
    guid = None
    sizemb = -1
    MAX_FILE_SIZE_MB = 1000

    _logger.debug("index started - " + datetime.now().strftime("%H:%M:%S") + "  ")

    # check if pandaid in URL - > get guid, scope and lfn needed to get job log tarball
    # check that pandaid in URL and it is valid
    pandaid = None
    if 'pandaid' in request.session['requestParams'] and request.session['requestParams']['pandaid'] is not None:
        try:
            pandaid = int(request.session['requestParams']['pandaid'])
        except ValueError:
            _logger.error('Provided pandaid is not numerical!')
    else:
        _logger.warning('No valid pandaid provided')

    if pandaid is not None:
        query = {
            'type': 'log',
            'pandaid': pandaid,
        }
        file_values = ('pandaid', 'guid','scope', 'lfn', 'status', 'fsize')
        file_properties = {}
        log_files = []
        log_files.extend(Filestable4.objects.filter(**query).values(*file_values))
        if len(file_properties) == 0:
            log_files.extend(FilestableArch.objects.filter(**query).values(*file_values))

        if len(log_files) == 0:
            _logger.warning('No log file found for the provided pandaid={}'.format(pandaid))
        elif len(log_files) > 0:
            file_properties = log_files[0]
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
            try:
                sizemb = round(int(file_properties['fsize'])/1000/1000)
            except:
                sizemb = 0

    else:
        # check that all expected parameters are in URL
        # 'site' is not mandatory anymore, so removing it from the list
        expectedFields = ['guid', 'scope', 'lfn']
        for expectedField in expectedFields:
            try:
                request.GET[expectedField]
            except:
                msg = 'Missing expected GET parameter %s. ' % expectedField
                _logger.error(msg)
                if 'missingparameter' not in errors.keys():
                    errors['missingparameter'] = ''
                errors['missingparameter'] += msg

        # if all expected GET parameters are present, execute file lookup
        pattern_string='^[a-zA-Z0-9.\-_]+$'
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

    # check if size of logfile is too big return to user error message with rucio cli command to download it locally
    if sizemb < 0:
        log_files = []
        try:
            fileid = int(request.GET['fileid'])
        except:
            fileid = -1
        lquery = {'type': 'log'}
        if lfn and len(lfn) > 0:
            lquery['lfn'] = lfn
            log_files.extend(Filestable4.objects.filter(**lquery).values('fsize', 'fileid', 'status'))
            if len(log_files) == 0:
                log_files.extend(FilestableArch.objects.filter(**lquery).values('fsize', 'fileid', 'status'))
            if len(log_files) > 0:
                try:
                    if fileid > 0:
                        sizemb = round(int([f['fsize'] for f in log_files if f['fileid'] == fileid][0])/1000/1000)
                    else:
                        sizemb = round(int([f['fsize'] for f in log_files][0])/1000/1000)
                except:
                    _logger.warning("ERROR!!! Failed to calculate log tarball size in MB")

    if sizemb > MAX_FILE_SIZE_MB:
        _logger.warning('Size of the requested log is {} MB which is more than limit {} MB'.format(sizemb, MAX_FILE_SIZE_MB))
        errormessage = """The size of requested log is too big ({}MB). 
                            Please try to download it locally using Rucio CLI by the next command: 
                            rucio download {}:{}""".format(sizemb, scope, lfn)
        data = {
            'errormessage': errormessage
        }
        return render(request, 'errorPage.html', data, content_type='text/html')

    _logger.debug("index step1 - " + datetime.now().strftime("%H:%M:%S") + "  ")

    ### download the file
    files = []
    dirprefix = ''
    tardir = ''

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
        return render(request, 'errorPage.html', data, content_type='text/html')
    if not len(files):
        msg = 'Something went wrong while the log file downloading. [guid={}, scope={}, lfn={}] \n'.format(guid, scope, lfn)
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
        totalLogSize = round(totalLogSize*1.0/1024/1024, 2)

    ### set request response data
    data = {
        'request': request,
        'errors': errors,
        'files': files,
        'dirprefix': dirprefix,
        'tardir': tardir,
        'scope': scope,
        'lfn': lfn,
        'guid': guid,
        'MEDIA_URL': settings.MEDIA_URL,
        'viewParams' : {'MON_VO': str(get_filebrowser_vo()).upper()},
        'HOSTNAME': hostname,
        'totalLogSize': totalLogSize,
        'nfiles': len(files),
    }

    _logger.debug("index step3 - " + datetime.now().strftime("%H:%M:%S") + "  ")
    if 'json' not in request.GET:
        status = 200
        # return 500 if most probably there were issue   
        if 'download' in errors and errors['download'] and len(errors['download']) > 0:
            if len(files) > 0 and 'status' in files[0] and files[0]['status'] != 'failed' and sizemb <= 0:
                status = 500
        return render(request, 'filebrowser/filebrowser_index.html', data, RequestContext(request), status=status)
    else:
        resp = HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
        _logger.debug("index step4 - " + datetime.now().strftime("%H:%M:%S") + "  ")
        return resp



def get_job_log_file_path(pandaid, filename=''):
    """
    Download log tarball of a job and return path to a local copy of memory_monitor_output.txt file
    If the directIO is enabled for prmon, return the remote location
    :param pandaid:
    :param filename: str, if empty the function returm path to tarball folder
    :return: file_path: str
    """

    if settings.PRMON_LOGS_DIRECTIO_LOCATION and filename in ('memory_monitor_summary.json','memory_monitor_output.txt'):
        joblist = get_job_list(query={"pandaid":pandaid})
        if joblist and len(joblist) > 0:
            computingsite = joblist[0].get('computingsite')
        return settings.PRMON_LOGS_DIRECTIO_LOCATION.format(queue_name = computingsite, panda_id = pandaid) + '/' + filename

    file_path = None
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
            tarball_path = get_fullpath_filebrowser_directory() + '/' + guid.lower() + '/' + scope + '/'
            files, err, tardir = list_file_directory(tarball_path, 100)
            _logger.debug('tarball path is {} \nError message is {} \nGot tardir: {}'.format(tarball_path, err, tardir))
            if len(files) == 0 and len(err) > 0:
                # download tarball
                _logger.debug('log tarball has not been downloaded, so downloading it now')
                files, errtxt, dirprefix, tardir = get_rucio_file(scope, lfn, guid)
                _logger.debug('Got files for dir: {} and tardir: {}. Error message: {}'.format(dirprefix, tardir, errtxt))
            if type(files) is list and len(files) > 0 and len(filename) > 0:
                for f in files:
                    if f['name'] == filename:
                        file_path = tarball_path + '/' + tardir + '/' + filename

    _logger.debug('Final path of {} file: {}'.format(filename, file_path))
    return file_path


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

