"""
    filebrowser.views
    
"""
import logging
import re
import json
import time
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.template import RequestContext
from django.conf import settings
from django.urls import reverse
from django.views.decorators.cache import never_cache

from core.filebrowser.utils import (get_rucio_file, remove_folder, get_job_log_file_properties, get_job_computingsite, get_s3_file,
                                    get_log_provider, extract_rucio_errors)
from core.oauth.utils import login_customrequired
from core.views import initRequest
from core.libs.exlib import convert_bytes
from core.libs.DateTimeEncoder import DateTimeEncoder
from core.utils import is_json_request
import core.filebrowser.constants as const

_logger = logging.getLogger('bigpandamon-filebrowser')
filebrowserDateTimeFormat = "%Y %b %d %H:%M:%S"

@login_customrequired
def index(request):
    """
        index -- filebrowser front page, it will check if logs exist and trigger download if needed.
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
    computingsite = None
    size_mb = -1
    files = []
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
            scope, lfn, guid, pandaid, size_mb = get_job_log_file_properties(pandaid=pandaid)
        else:
            # to get log file properties like size_mb we need to at least lfn
            if 'lfn' in request.session['requestParams']:
                lfn = request.session['requestParams']['lfn']
                scope, lfn, guid, pandaid, size_mb = get_job_log_file_properties(lfn=lfn)
            else:
                lfn = None
                if 'missingparameter' not in errors.keys():
                    errors['missingparameter'] = ''
                errors['missingparameter'] += 'Missing required lfn parameter. '

        # check if size of logfile is too big return to user error message with rucio cli command to download it locally
        if size_mb > const.LOG_SIZE_MB_MAX:
            _logger.warning('Size of the requested log is {} MB which is more than limit {} MB'.format(size_mb, const.LOG_SIZE_MB_MAX))
            errors['too_big_tarball'] = f"The size of requested log is too big ({size_mb}MB) to download and browse interactively. " + \
                f"Please try to download it on lxplus or locally via Rucio CLI using the following command: rucio get {scope}:{lfn}"
        elif size_mb > const.LOG_SIZE_MB_THRESHOLD:
            _logger.info(f"""Size of the requested log is {size_mb} MB which is more than threshold {const.LOG_SIZE_MB_THRESHOLD} MB. 
                Informing user that it will take time to load""")
            errors['slow_downloading'] = f"The size of job log tarball is quite big ({size_mb}MB)."
        api_url = reverse('filebrowser-list-rucio', kwargs={'provider': log_provider, 'scope': scope, 'lfn': lfn, 'guid': guid})
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
        api_url = reverse('filebrowser-list-s3', kwargs={'provider': log_provider, 'pandaid': pandaid, 'computingsite': computingsite})
    else:
        api_url = ''
        errors['download'] = f"{log_provider} log provider is not supported."

    _logger.debug('Data prepared to render - {}'.format(time.time() - request.session['req_init_time']))
    if not is_json_request(request):
        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            'errors': errors,
            'api_url': api_url
        }
        response = render(request, 'filebrowser_index.html', data, RequestContext(request))
        _logger.debug('Rendered template - {}'.format(time.time() - request.session['req_init_time']))
        return response
    else:
        # for JSON requests we actually need to load log files
        if 'missingparameter' in errors or 'improperformat' in errors:
            return JsonResponse({'error': ' '.join([errors['missingparameter']] if 'missingparameter' in errors else [])}, status=400)
        if log_provider == 'rucio':
            # if all expected parameters are present and valid, execute file lookup
            if guid is None or lfn is None or scope is None:
                errormessage = 'Not enough params provided to download logs or pandaid does not exist. {} {}'.format(
                    errors['missingparameter'] if 'missingparameter' in errors else '',
                    errors['improperformat'] if 'improperformat' in errors else ''
                )
                _logger.warning(errormessage)
                return JsonResponse({'error': errormessage}, status=400)
            # download the file
            files, errtxt, dirprefix, tardir = get_rucio_file(scope, lfn, guid, limit=100)
        elif log_provider == 's3':
            if not pandaid or not computingsite:
                errormessage = 'Not enough params provided to download logs or pandaid does not exist. {} {}'.format(
                    errors['missingparameter'] if 'missingparameter' in errors else '',
                    errors['improperformat'] if 'improperformat' in errors else ''
                )
                _logger.warning(errormessage)
                return JsonResponse({'error': errormessage}, status=400)
            files, errtxt, dirprefix, tardir = get_s3_file(pandaid, computingsite)
        else:
            errtxt = f"Can not load log files using {log_provider} provider"

        if len(errtxt) > 0:
            errors['download'] = f'Something went wrong while the log file downloading: \n{errtxt}'
            _logger.warning(errors['download'])
            return JsonResponse({'error': errors['download']}, status=400)

        # add media path to each file
        media_path = f"{request.get_host()}/{settings.MEDIA_URL}{dirprefix}{tardir}"
        if not media_path.endswith('/'):
            media_path += '/'
        for file in files:
            file['media_link'] = f"{media_path}{file['dirname']}{file['name']}"
        if 'filename' in request.session['requestParams'] and request.session['requestParams']['filename'] is not None:
            if request.session['requestParams']['filename'] in [f['name'] for f in files]:
                response = redirect(f"https://{media_path}{request.session['requestParams']['filename']}", permanent=True)
            else:
                response = JsonResponse({'error': "No such log file found in tarball"}, status=404)
        else:
            response = JsonResponse({'error': ';'.join([f"{e}:{m}" for e, m in errors.items()]), 'files': files},
                encoder=DateTimeEncoder,
                safe=False
            )
        return response


@never_cache
def load_log_file_list(request, provider="rucio", guid=None, scope=None, lfn=None, pandaid=None, computingsite=None):
    """
    Load log files from the log provider (rucio or s3) and return the list of files.
    :param request: HTTP request, which should contain the following parameters:
    :param provider: log provider to use (rucio or s3)
    :param guid: GUID of the file (for rucio)
    :param scope: scope of the file (for rucio)
    :param lfn: logical file name of the file (for rucio)
    :param pandaid: PanDA ID of the job (for s3)
    :param computingsite: computing site of the job (for s3)
    :return:
        JsonResponse with the list of files, total unpacked size, and number of files.
        If the request is not valid, an error response will be returned.
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    files = []
    errors = {}
    dirprefix = ''
    tardir = ''

    if provider is not None and provider == 'rucio':
        # if all expected parameters are present and valid, execute file lookup
        pattern_string = r'^[a-zA-Z0-9.\-_]+$'
        pattern_guid = r'^(\{){0,1}[0-9a-zA-Z]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\}){0,1}$'
        if guid is None or re.match(pattern_guid, guid) is None:
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += f'guid: {guid}'
            guid = None
        if lfn is None or re.match(pattern_string, lfn) is None:
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += f'lfn: {lfn} '
            lfn = None
        if scope is None or re.match(pattern_string, scope) is None:
            if 'improperformat' not in errors.keys():
                errors['improperformat'] = ''
            errors['improperformat'] += f'scope: {scope} '
            scope = None
        # download the file
        if not (guid is None or lfn is None or scope is None):
            files, errtxt, dirprefix, tardir = get_rucio_file(scope, lfn, guid, limit=100)
            if len(errtxt) > 0:
                errors['download'] = errtxt
            _logger.debug('Downloading of logs finished - {}'.format(time.time() - request.session['req_init_time']))
        else:
            errormessage = 'Not enough params provided to download logs or pandaid does not exist. {} {}'.format(
                errors['missingparameter'] if 'missingparameter' in errors else '',
                errors['improperformat'] if 'improperformat' in errors else ''
            )
            _logger.warning(errormessage)
            return JsonResponse({'error': errormessage}, status=400)

    elif provider is not None and provider == 's3':
        try:
            pandaid = int(pandaid)
        except ValueError:
            pandaid = None
            msg = 'Provided pandaid is not integer!'
            errors['improperformat'] = msg
            _logger.warning(msg)
        if pandaid and not computingsite:
            computingsite = get_job_computingsite(pandaid)  # try to get it from PanDA DB by pandaid
        if not computingsite:
            errors['improperformat'] = 'No computingsite provided or found for a job'

        if pandaid and computingsite:
            files, err_txt, dirprefix, local_dir = get_s3_file(pandaid, computingsite)
            if len(err_txt) > 0:
                errors['download'] = err_txt
            _logger.debug('Got list of files from object store - {}'.format(time.time() - request.session['req_init_time']))
        else:
            errormessage = 'Not enough params provided to download logs or pandaid does not exist. {} {}'.format(
                errors['missingparameter'] if 'missingparameter' in errors else '',
                errors['improperformat'] if 'improperformat' in errors else ''
            )
            _logger.warning(errormessage)
            return JsonResponse({'error': errormessage}, status=400)
    else:
        errors['download'] = f'Can not load log files using {provider}'

    if 'download' in errors:
        extracted_errors = extract_rucio_errors(errors["download"], include_warnings=False)
        errors["download"] = f'Something went wrong while the log file downloading: \n{extracted_errors if len(extracted_errors) > 0 else errors["download"]}'

    media_path = f"{request.get_host()}/{settings.MEDIA_URL}{dirprefix}{tardir}"
    if not media_path.endswith('/'):
        media_path += '/'

    # calculate total size of log files
    total_unpacked_size = 0
    if type(files) is list and len(files) > 0:
        for file in files:
            total_unpacked_size += file['size'] if 'size' in file and file['size'] > 0 else 0
        # from B to MB
        total_unpacked_size = round(convert_bytes(total_unpacked_size, output_unit='MB'), 2)

    return JsonResponse(
        {'errors': errors, 'files': files, 'total_unpacked_size': total_unpacked_size, 'media_path': media_path},
        encoder=DateTimeEncoder,
        safe=False
    )



def delete_files(request):
    """
    Clear subfolder containing log files
    :param request:
    :return:
    """
    # check that path to logs is provided
    guid = None
    try:
        guid = request.GET['guid']
    except:
        msg = 'Missing guid GET parameter'
        _logger.error(msg)

    # clean folder if guid provided
    if guid is not None:
        logdir = remove_folder(guid)
        data = {'message':'The folder was cleaned ' + logdir}
        return HttpResponse(json.dumps(data), content_type='application/json')
    else:
        return HttpResponse(status=404)

