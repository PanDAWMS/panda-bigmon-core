import json, random

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render_to_response

from core.views import login_customrequired, initRequest,  DateTimeEncoder

from core.grafana.Grafana import Grafana
from core.grafana.Query import Query
from core.grafana.data_tranformation import stacked_hist
from core.grafana.Headers import Headers

colours_codes = {
    "0": "#AE3C51",
    "1": "#6298FF",
    "2": "#D97529",
    "3": "#009246",
    "AOD": "#006019",
    "Analysis": "#FF00FF",
    "CA": "#FF1F1F",
    "CAF processing": "#CAD141",
    "CERN": "#AE3C51",
    "Custodial": "#FF0000",
    "DE": "#000000",
    "DESD": "#4189FF",
    "DPD": "#FEF100",
    "Data Processing": "#FFFF00",
    "Data Processing (XP)": "#008800",
    "Default": "#808080",
    "ES": "#EDBF00",
    "ESD": "#001640",
    "Extra Production": "#FF0000",
    "FR": "#0055A5",
    "Group Analysis": "#808080",
    "Group Production": "#008800",
    "HITS": "#FF6666",
    "IT": "#009246",
    "MC Event Generation": "#356C20",
    "MC Production": "#0000FF",
    "MC Reconstruction": "#00006B",
    "MC Reconstruction (XP)": "#D97529",
    "MC Simulation": "#0000FF",
    "MC Simulation (XP)": "#AE3C51",
    "MC Simulation Fast": "#0099CC",
    "MC Simulation Fast (XP)": "#0099CC",
    "MC Simulation Full": "#00CCCC",
    "MC Simulation Full (XP)": "#00CCCC",
    "ND": "#6298FF",
    "NL": "#D97529",
    "Other": "#66008D",
    "Others": "#00FFFF",
    "Others (XP)": "#009246",
    "Primary": "#FFA500",
    "RAW": "#FF0000",
    "RU": "#66008D",
    "Rest": "#625D5D",
    "Secondary": "#00FFFF",
    "T0 processing": "#DB9900",
    "TW": "#89000F",
    "Testing": "#00FF00",
    "ToBeDeleted": "#FFFF00",
    "UK": "#356C20",
    "UNKNOWN": "#FFA500",
    "US": "#00006B",
    "User Analysis": "#FF00FF",
    "Validation": "#000000",
    "analysis": "#FF0000",
    "bstream": "#0055A5",
    "cancelled": "#FF9933",
    "closed": "#808080",
    "evgen": "#D97529",
    "evgentx": "#AE3C51",
    "failed": "#bf1b00",
    "filter": "#DB9900",
    "finished": "#248F24",
    "ganga": "#1433CC",
    "gangarobot": "#006666",
    "gangarobot-64": "#009999",
    "gangarobot-filestager": "#00CCCC",
    "gangarobot-new": "#00FFFF",
    "gangarobot-nightly": "#99FF00",
    "gangarobot-pft": "#99CC33",
    "gangarobot-pft-trial": "#999966",
    "gangarobot-rctest": "#996699",
    "gangarobot-root": "#CC0000",
    "gangarobot-squid": "#CC0066",
    "gangarobotnew": "#CC3399",
    "hammercloud": "#A5D3CA",
    "merge": "#FFA600",
    "merging": "#47D147",
    "non-panda_analysis": "#CCCCCC",
    "pandamover": "#FFE920",
    "pile": "#FF00FF",
    "prod_test": "#B4D1B6",
    "production": "#CAD141",
    "ptest": "#89C7FF",
    "rc_test": "#A5FF8A",
    "reco": "#00006B",
    "reprocessing": "#008800",
    "running": "#47D147",
    "simul": "#0000FF",
    "software": "#FFCFA4s",
    "t0_caf": "#CAD141",
    "t0_processing": "#FFA600",
    "test": "#00FF00",
    "transfering": "#47D147",
    "txtgen": "#29AFD6",
    "validation": "#000000"
}

@login_customrequired
def index(request):
    """The main page containing drop-down menus to select group by options etc.
    Data delivers asynchroniously by request to grafana_api view"""

    valid, response = initRequest(request)

    # all possible group by options and plots to build
    group_by = {'dst_federation': 'Federation'}
    split_series = {'adcactivity': 'ADC Activity', 'jobstatus': 'Job status'}
    plots = {'cpuconsumption': 'CPU Consumption', 'wallclockhepspec06':'WallClock HEPSPEC06'}

    data = {
        'group_by': group_by,
        'split_series': split_series,
        'plots': plots,
    }

    response = render_to_response('grafana-api-plots.html', data, content_type='text/html')
    return response

def chartjs(request):
    """The main page containing drop-down menus to select group by options etc.
    Data delivers asynchroniously by request to grafana_api view"""

    valid, response = initRequest(request)

    # all possible group by options and plots to build
    group_by = {'dst_federation': 'Federation'}
    split_series = {'adcactivity': 'ADC Activity', 'jobstatus': 'Job status'}
    plots = {'cpuconsumption': 'CPU Consumption', 'wallclockhepspec06':'WallClock HEPSPEC06'}

    data = {
        'group_by': group_by,
        'split_series': split_series,
        'plots': plots,
    }

    response = render_to_response('grafana-chartjs-plots.html', data, content_type='text/html')
    return response


def grafana_api(request):

    valid, response = initRequest(request)

    group_by = None
    split_series = None
    if 'groupby' in request.session['requestParams']:
        groupby_params = request.session['requestParams']['groupby'].split(',')
        if 'time' in groupby_params:
            pass
        else:
            group_by = groupby_params[0]
            if len(groupby_params) > 1:
                split_series = groupby_params[1]

    result = []

    q = Query()
    q = q.request_to_query(request)
    sum_pledges  = Query(agg_func='last', table='pledges', field='atlas', grouping='real_federation')

    #sum_pledges = Query(agg_func='sum', table='pledges', field='atlas', grouping='time(1m),real_federation')
    try:
        result = Grafana().get_data(q)
        #last_pledges = Grafana().get_data(last_pledges)
        sum_pledges = Grafana().get_data(sum_pledges)
        if 'type' in request.session['requestParams'] and request.session['requestParams']['type'] == 'd3js':
            data = stacked_hist(result['results'][0]['series'], group_by, split_series)
            return JsonResponse(data)
        if 'type' in request.session['requestParams'] and request.session['requestParams']['type'] == 'chartjs':
            data = {}
            data = stacked_hist(result['results'][0]['series'], group_by, split_series)
            sum_pledges = stacked_hist(sum_pledges['results'][0]['series'], 'real_federation')
            lables = list(data.keys())
            pledges_keys = list(sum_pledges.keys())
            datasets = []
            elements = {}

            for object in data:
                for element in data[object]:
                    elements.setdefault(element,[]).append(data[object][element])
                if object in pledges_keys:
                    elements.setdefault('pledges',[]).append(sum_pledges[object]['all']*7*24*60*60)
                else:
                    elements.setdefault('pledges', []).append(0)

            background = ''
            for key in elements:
                if key in colours_codes:
                   background = colours_codes[key]
                else:
                    r = lambda: random.randint(0, 255)
                    background = '#%02X%02X%02X' % (r(),r(),r())
                if key != 'pledges':
                    datasets.append({'label': key,'stack':'Stack 0','data': elements[key], 'backgroundColor': background})
                else:
                    datasets.append(
                {'label': key, 'stack': 'Stack 1', 'data': elements[key], 'backgroundColor': '#FF0000'})
            data = {'labels': lables, 'datasets': datasets}
            return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
    except Exception as ex:
        result.append(ex)
    return JsonResponse(result)


