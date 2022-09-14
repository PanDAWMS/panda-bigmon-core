import tarfile, os
import tempfile
import logging
from idds.client.client import Client
from core.views import initRequest
from django.http import JsonResponse, HttpResponse

from os.path import basename
from core.filebrowser import ruciowrapper
from core.libs.exlib import getDataSetsForATask

from django.conf import settings

_logger = logging.getLogger('bigpandamon')


def downloadlog(request):
    logfile = None
    initRequest(request)
    if 'workloadid' in request.session['requestParams']:
        workloadid = int(request.session['requestParams']['workloadid'])
        with tempfile.TemporaryDirectory() as dirpath:
            try:
                c = Client(host=settings.IDDS_HOST)
                logfile = c.download_logs(workload_id=workloadid, request_id=None, dest_dir=dirpath)
            except Exception as e:
                _logger.exception('Failed to download logs with iDDS client: {}'.format(e))

            if logfile:
                return getfile(logfile)
            return JsonResponse({'error': 'no log file supplied by idds module'}, safe=False)
    else:
        return JsonResponse({'error': 'no workloadid provided'}, safe=False)


def get_hpo_metrics_ds(taskid):
    datasets = getDataSetsForATask(taskid, type='output')
    # we assume here only one output dataset
    if (len(datasets) > 0):
        return datasets[0]['datasetname']


def archive_metric_files(basedir):
    with tarfile.open(basedir +'/'+ 'download.tar.gz', 'w') as archive:
        for i in os.listdir(basedir):
            if 'metric' in i:
                archive.add(basedir+'/'+i, arcname=i)


def downloadhpometrics(request):
    initRequest(request)
    if 'workloadid' in request.session['requestParams']:
        workloadid = int(request.session['requestParams']['workloadid'])
        ds_name = get_hpo_metrics_ds(workloadid)
        rw = ruciowrapper.ruciowrapper()
        down_results = rw.download_ds(ds_name)
        if 'basedir' in down_results:
            archive_metric_files(down_results['basedir'])
            return getfile(down_results['basedir'] + '/download.tar.gz')
        else:
            return JsonResponse(down_results, safe=False)


def getfile(file_location):
    try:
        with open(file_location, 'rb') as f:
           file_data = f.read()
        response = HttpResponse(file_data, content_type='application/gzip')
        response['Content-Disposition'] = 'attachment; filename='+basename(file_location)
    except IOError:
        response = JsonResponse({'error': 'file not exists'}, safe=False)

    return response
