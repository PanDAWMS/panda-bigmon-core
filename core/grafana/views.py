import json

from django.http import HttpResponse
from django.shortcuts import render_to_response

from core.views import login_customrequired, initRequest,  DateTimeEncoder

from core.grafana.Grafana import Grafana
from core.grafana.Query import Query
from core.grafana.data_tranformation import stacked_hist
from core.grafana.Headers import Headers

@login_customrequired
def index(request):
    """The main page containing drop-down menus to select group by options etc.
    Data delivers asynchroniously by request to grafana_api view"""

    valid, response = initRequest(request)

    # all possible group by options and plots to build
    group_by = {'dst_federation': 'Federation'}
    split_series = {'adcactivity': 'ADC Activity'}
    plots = {'cpuconsumption': 'CPU Consumption'}

    data = {
        'group_by': group_by,
        'split_series': split_series,
        'plots': plots,
    }

    response = render_to_response('grafana-api-plots.html', data, content_type='text/html')
    return response


def grafana_api(request):
    valid, response = initRequest(request)

    group_by = None
    split_series = None
    if 'groupby' in request.session['requestParams']:
        groupby_params = request.session['requestParams']['groupby'].split(',')
        group_by = groupby_params[0]
        if len(groupby_params) > 1:
            split_series = groupby_params[1]

    result = []

    q = Query()
    q = q.request_to_query(request)
    try:
        result = Grafana().get_data(q)
        data = stacked_hist(result['results'][0]['series'], group_by, split_series)
    except Exception as ex:
        result.append(ex)



    return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')


