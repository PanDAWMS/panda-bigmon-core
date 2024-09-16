"""
    filebrowser.views
    
"""
import logging
import re
import json
import time
from django.http import HttpResponse
from django.shortcuts import render
from django.template import RequestContext
from django.conf import settings
from core.filebrowser.utils import get_rucio_file, remove_folder, get_job_log_file_properties, get_job_computingsite, get_s3_file, get_log_provider
from core.oauth.utils import login_customrequired
from core.views import initRequest
from core.libs.exlib import convert_bytes
from core.libs.DateTimeEncoder import DateTimeEncoder
from core.utils import is_json_request, error_response

_logger = logging.getLogger('bigpandamon-filebrowser')
filebrowserDateTimeFormat = "%Y %b %d %H:%M:%S"

@login_customrequired
def index(request):
    """
        index -- filebrowser's default page
        Download logs from the log provider and return the list of files
        The log provider can be set in settings (rucio or objectstore) which is used for all jobs.
            If it is 'cric' - > we check computingsite acopytools config
            The default provider is 'rucio'
        :param request: Django's HTTP request
        Errors dictionary can contain the following types of error messages:
            - missingparameter
            - improperformat
            - download
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    errors = {}
    scope = None
    lfn = None
    guid = None
    pandaid = None
    computingsite = None
    sizemb = -1
    MAX_FILE_SIZE_MB = 1000
    files = []
    dirprefix = ''
    tardir = ''
    _logger.debug('Filebrowser started - {}'.format(time.time() - request.session['req_init_time']))

    pandaid = None
    if 'pandaid' in request.session['requestParams'] and request.session['requestParams']['pandaid'] is not None:
        try:
            pandaid = int(request.session['requestParams']['pandaid'])
            request.session['viewParams']['PanDA ID'] = pandaid
        except ValueError:
            msg = 'Provided pandaid is not integer!'
            errors['improperformat'] = msg
            _logger.error('Provided pandaid is not numerical!')

    # get log_provider
    log_provider = get_log_provider(pandaid=pandaid)

    if log_provider == 'rucio':
        # check if pandaid in URL - > get guid, scope and lfn needed to get job log tarball
        # check that pandaid in URL, and it is valid
        if pandaid is not None:
            scope, lfn, guid, pandaid, sizemb = get_job_log_file_properties(pandaid=pandaid)
        else:
            # check that all expected parameters are in URL
            expectedFields = ['guid', 'scope', 'lfn']
            for expectedField in expectedFields:
                if expectedField not in request.session['requestParams']:
                    msg = 'Missing expected GET parameter {}. '.format(expectedField)
                    _logger.error(msg)
                    if 'missingparameter' not in errors.keys():
                        errors['missingparameter'] = ''
                    errors['missingparameter'] += msg

            # if all expected GET parameters are present, execute file lookup
            pattern_string=r'^[a-zA-Z0-9.\-_]+$'
            pattern_guid=r'^(\{){0,1}[0-9a-zA-Z]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\}){0,1}$'
            if 'guid' in request.session['requestParams']:
                guid = request.session['requestParams']['guid']
                if re.match(pattern_guid, guid) is None:
                    guid = None
                    if 'improperformat' not in errors.keys():
                        errors['improperformat'] = ''
                    errors['improperformat'] += 'guid: %s ' % (request.GET['guid'])
                if guid:
                    request.session['viewParams']['guid'] = guid
            if 'lfn' in request.session['requestParams']:
                lfn = request.session['requestParams']['lfn']
                if re.match(pattern_string, lfn) is None:
                    lfn = None
                    if 'improperformat' not in errors.keys():
                        errors['improperformat'] = ''
                    errors['improperformat'] += 'lfn: %s ' % (request.GET['lfn'])
                if lfn:
                    request.session['viewParams']['lfn'] = lfn
            if 'scope' in request.session['requestParams']:
                scope = request.session['requestParams']['scope']
                if re.match(pattern_string, scope) is None:
                    scope = None
                    if 'improperformat' not in errors.keys():
                        errors['improperformat'] = ''
                    errors['improperformat'] += 'scope: %s ' % (request.GET['scope'])
                if scope:
                    request.session['viewParams']['scope'] = scope

            # get size of log tarball
            if lfn:
                _, _, _, _, sizemb = get_job_log_file_properties(lfn=lfn)

        # check if size of logfile is too big return to user error message with rucio cli command to download it locally
        if sizemb > MAX_FILE_SIZE_MB:
            _logger.warning('Size of the requested log is {} MB which is more than limit {} MB'.format(sizemb, MAX_FILE_SIZE_MB))
            errormessage = f"""The size of requested log is too big ({sizemb}MB). 
                                Please try to download it locally using Rucio CLI by the next command: 
                                rucio download {scope}:{lfn}"""
            return error_response(request, message=errormessage, status=400)
        _logger.debug('Prepared params needed to download logs with rucio - {}'.format(time.time() - request.session['req_init_time']))

        # download the file
        if not (guid is None or lfn is None or scope is None):
            files, errtxt, dirprefix, tardir = get_rucio_file(scope, lfn, guid, 100)
        else:
            errormessage = 'Not enough params provided to download logs or pandaid does not exist. {} {}'.format(
                errors['missingparameter'] if 'missingparameter' in errors else '',
                errors['improperformat'] if 'improperformat' in errors else ''
            )
            _logger.warning(errormessage)
            return error_response(request, message=errormessage, status=400)
        if not len(files):
            msg = 'Something went wrong while the log file downloading. [guid={}, scope={}, lfn={}] \n'.format(guid, scope, lfn)
            _logger.warning(msg)
            errors['download'] = msg
        if len(errtxt):
            if 'download' not in errors:
                errors['download'] = ''
            errors['download'] += errtxt

        _logger.debug('Downloading of logs finished - {}'.format(time.time() - request.session['req_init_time']))

    elif log_provider == 's3':

        if not pandaid:
            msg = 'No pandaid provided'
            errors['missingparameter'] = msg

        if 'computingsite' in request.session['requestParams']:
            computingsite = request.session['requestParams']['computingsite']
        else:
            # try to get it from PanDA DB by pandaid
            if pandaid and not computingsite:
                computingsite = get_job_computingsite(pandaid)
            if not computingsite :
                msg = 'No computingsite provided or found for a job'
                errors['missingparameter'] = msg
        if computingsite:
            request.session['viewParams']['computingsite'] = computingsite

        if pandaid and computingsite:
            files, err_txt, dirprefix, local_dir = get_s3_file(pandaid, computingsite)
            if len(err_txt) > 0:
                errors['download'] = err_txt
            _logger.debug('Got list of files from object store - {}'.format(time.time() - request.session['req_init_time']))
    else:
        errors['download'] = 'Can not load log files using {}'.format(log_provider)

    # calculate total size of log files
    totalLogSize = 0
    if type(files) is list and len(files) > 0:
        for file in files:
            totalLogSize += file['size'] if 'size' in file and file['size'] > 0 else 0
        # from B to MB
        totalLogSize = round(convert_bytes(totalLogSize, output_unit='MB'), 2)
    _logger.debug('Data prepared to render - {}'.format(time.time() - request.session['req_init_time']))

    if not is_json_request(request):

        request.session['urls_cut']['media_base_link'] = '{}/{}{}{}'.format(
            request.get_host(), settings.MEDIA_URL, dirprefix, tardir
        )
        if not request.session['urls_cut']['media_base_link'].endswith('/'):
            request.session['urls_cut']['media_base_link'] += '/'

        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            'errors': errors,
            'files': files,
            'totalLogSize': totalLogSize,
            'nfiles': len(files),
        }
        response = render(request, 'filebrowser_index.html', data, RequestContext(request))
        _logger.debug('Rendered template - {}'.format(time.time() - request.session['req_init_time']))
        return response
    else:
        response = HttpResponse(json.dumps(files, cls=DateTimeEncoder), content_type='application/json')
        return response


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

