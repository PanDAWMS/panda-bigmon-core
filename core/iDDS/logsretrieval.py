from idds.client.client import Client
from core.views import initRequest
from django.http import JsonResponse, HttpResponse, HttpResponseNotFound
import tempfile
from os.path import basename
from core.settings import IDDS_HOST

c = Client(IDDS_HOST)

def downloadlog(request):
    initRequest(request)
    if 'workloadid' in request.session['requestParams']:
        workloadid = int(request.session['requestParams']['workloadid'])
        with tempfile.TemporaryDirectory() as dirpath:
            logfile = c.download_logs(workload_id=workloadid, request_id=None, dest_dir=dirpath)
            if logfile:
                return getfile(logfile)
            return JsonResponse({'error': 'no log file supplied by idds module'}, safe=False)

    else:
        return JsonResponse({'error': 'no workloadid provided'}, safe=False)



def getfile(file_location):
    try:
        with open(file_location, 'rb') as f:
           file_data = f.read()
        response = HttpResponse(file_data, content_type='application/gzip')
        response['Content-Disposition'] = 'attachment; filename='+basename(file_location)
    except IOError:
        response = JsonResponse({'error': 'file not exists'}, safe=False)

    return response
