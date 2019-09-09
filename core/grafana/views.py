import json, random
from datetime import datetime, timedelta

import hashlib
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render_to_response
from django.template import loader
from django.utils import encoding
from django.utils.cache import patch_response_headers

from core.views import login_customrequired, initRequest, DateTimeEncoder, endSelfMonitor, DateEncoder

from core.grafana.Grafana import Grafana
from core.grafana.Query import Query
from core.grafana.data_tranformation import stacked_hist, pledges_merging
from core.grafana.Headers import Headers

from core.libs.cache import setCacheEntry, getCacheEntry

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
    last_pledges = Query(agg_func='last', table='pledges_last', field='value', grouping='real_federation')
    #/ api / datasources / proxy / 9267 / query?db = monit_production_rebus
    #sum_pledges = Query(agg_func='sum', table='pledges', field='atlas', grouping='time(1m),real_federation')
    try:
        if q.table == 'pledges_last' or q.table == 'pledges_sum' or q.table == 'pledges_hs06sec':
            result = Grafana(database='monit_production_rebus').get_data(q)
        else:
            result = Grafana().get_data(q)
        #last_pledges = Grafana().get_data(last_pledges)

        if 'type' in request.session['requestParams'] and request.session['requestParams']['type'] == 'd3js':
            data = stacked_hist(result['results'][0]['series'], group_by, split_series)
            return JsonResponse(data)
        if 'type' in request.session['requestParams'] and request.session['requestParams']['type'] == 'chartjs':
            last_pledges = Grafana(database='monit_production_rebus').get_data(last_pledges)
            data = {}
            data = stacked_hist(result['results'][0]['series'], group_by, split_series)
            last_pledges = stacked_hist(last_pledges['results'][0]['series'], 'real_federation')
            lables = list(data.keys())
            pledges_keys = list(last_pledges.keys())
            datasets = []
            elements = {}

            for object in data:
                for element in data[object]:
                    elements.setdefault(element,[]).append(data[object][element])
                if object in pledges_keys:
                    elements.setdefault('pledges',[]).append(last_pledges[object]['all']*7*24*60*60)
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
        if 'export' in request.session['requestParams']:
            if request.session['requestParams']['export'] == 'csv':
                data = stacked_hist(result['results'][0]['series'], group_by, split_series)

                import csv
                import copy
                response = HttpResponse(content_type='text/csv')

                column_titles = copy.deepcopy(groupby_params)
                column_titles.append('value')

                response['Content-Disposition'] = 'attachment; filename={0}.csv'.format('_'.join(groupby_params))
                writer = csv.writer(response, delimiter=";")

                writer.writerow(column_titles)
                csvList = []
                if len(groupby_params)> 1:
                    csvList = grab_children(data)
                else:
                    for key,value in data.items():
                        csvList.append([key,value['all']])
                writer.writerows(csvList)

                return response


    except Exception as ex:
        result.append(ex)
    return JsonResponse(result)

def grab_children(data,parent=None,child=None):
    if child is None:
        child = []
    for key, value in data.items():
        if isinstance(value,dict):
            grab_children(value,key,child)
        else:
            child.append([parent,key,value])
    return child

def pledges(request):
    valid, response = initRequest(request)

    if 'date_from' in request.session['requestParams'] and 'date_to' in request.session['requestParams']:
        starttime = request.session['requestParams']['date_from']
        endtime = request.session['requestParams']['date_to']
        date_to = datetime.strptime(endtime,"%d.%m.%Y %H:%M:%S")
        date_from = datetime.strptime(starttime, "%d.%m.%Y %H:%M:%S")
        total_seconds = (date_to-date_from).total_seconds()
        total_days = (date_to-date_from).days
        date_list = []
        if (date_to-date_from).days > 30:
            n = 20
            while True:
                start_date = date_from
                end_date = (start_date + timedelta(days=n))
                end_date = end_date - timedelta(minutes=1)
                if end_date >= date_to:
                    end_date = date_to - timedelta(minutes=1)
                    date_list.append([start_date.strftime("%d.%m.%Y %H:%M:%S"),end_date.strftime("%d.%m.%Y %H:%M:%S")])
                    break
                else:
                    date_list.append([start_date.strftime("%d.%m.%Y %H:%M:%S"), end_date.strftime("%d.%m.%Y %H:%M:%S")])
                    date_from = end_date + timedelta(minutes=1)
        else:
            newendtime = (date_to - timedelta(minutes=1)).strftime("%d.%m.%Y %H:%M:%S")
            date_list.append([starttime, newendtime])

    else:
        timebefore = timedelta(days=7)
        endtime = (datetime.utcnow()).replace(minute=00, hour=00, second=00, microsecond=000)
        starttime = (endtime - timebefore).replace(minute=00, hour=00, second=00, microsecond=000)
        total_seconds = (starttime - endtime).total_seconds()
        endtime = endtime - timedelta(minutes=1)
        endtime = endtime.strftime("%d.%m.%Y %H:%M:%S")
        starttime = starttime.strftime("%d.%m.%Y %H:%M:%S")

    if 'type' in request.session['requestParams'] and request.session['requestParams']\
        ['type'] == 'federation':

        key = hashlib.md5(encoding.force_bytes("{0}_{1}_federation".format(starttime, endtime)))
        key = key.hexdigest()
        federations = getCacheEntry(request, key, isData=True)
        if federations is not None:
            federations = json.loads(federations)
            return HttpResponse(json.dumps(federations), content_type='text/json')

        pledges_dict = {}
        pledges_list = []

        if len(date_list)>1:
            for date in date_list:
                hs06sec = Query(agg_func='sum', table='completed', field='sum_hs06sec',
                                grouping='time,dst_federation', starttime=date[0], endtime=date[1])

                hs06sec = Grafana().get_data(hs06sec)

                pledges_sum = Query(agg_func='mean', table='pledges_hs06sec', field='value',
                                grouping='time,real_federation', starttime=date[0], endtime=date[1])
                pledges_sum = Grafana(database='monit_production_rebus').get_data(pledges_sum)
                pledges_dict = pledges_merging(hs06sec, pledges_sum, total_seconds,
                                                             pledges_dict)
        else:
            hs06sec = Query(agg_func='sum', table='completed', field='sum_hs06sec',
                            grouping='time,dst_federation', starttime=date_list[0][0], endtime=date_list[0][1])

            hs06sec = Grafana().get_data(hs06sec)

            pledges_sum = Query(agg_func='mean', table='pledges_hs06sec', field='value',
                                grouping='time,real_federation', starttime=date_list[0][0], endtime=date_list[0][1])
            pledges_sum = Grafana(database='monit_production_rebus').get_data(pledges_sum)
            pledges_dict = pledges_merging(hs06sec, pledges_sum, total_seconds,
                                                         pledges_dict)
        for pledges in pledges_dict:
            if pledges == 'NULL':
                continue
            else:
                # pledges_list.append(
                #     {type: pledges, "hs06sec": pledges_dict[pledges]['hs06sec'],
                #                    'pledges': pledges_dict[pledges]['pledges']})
                pledges_list.append({"dst_federation":pledges, "hs06sec":int(round(float(pledges_dict[pledges]['hs06sec'])/86400, 2)),
                                      'pledges': int(round(float(pledges_dict[pledges]['pledges'])/86400, 2))})
        setCacheEntry(request, key, json.dumps(pledges_list), 60 * 60 * 24 * 30, isData=True)
        return HttpResponse(json.dumps(pledges_list), content_type='text/json')
    elif 'type' in request.session['requestParams'] and request.session['requestParams']\
        ['type'] == 'country':

        key = hashlib.md5(encoding.force_bytes("{0}_{1}_country".format(starttime, endtime)))
        key = key.hexdigest()
        countries = getCacheEntry(request, key, isData=True)
        if countries is not None:
            countries = json.loads(countries)
            return HttpResponse(json.dumps(countries), content_type='text/json')

        pledges_dict = {}
        pledges_list = []
        if len(date_list)>1:
            for date in date_list:
                hs06sec = Query(agg_func='sum', table='completed', field='sum_hs06sec',
                                    grouping='time,dst_federation,dst_country', starttime=date[0], endtime=date[1])
                hs06sec = Grafana().get_data(hs06sec)

                pledges_sum = Query(agg_func='mean', table='pledges_hs06sec', field='value',
                                    grouping='time,real_federation,country', starttime=date[0], endtime=date[1])
                pledges_sum = Grafana(database='monit_production_rebus').get_data(pledges_sum)
                pledges_dict = pledges_merging(hs06sec, pledges_sum, total_seconds, pledges_dict,
                                                              type='dst_country')
        else:
            hs06sec = Query(agg_func='sum', table='completed', field='sum_hs06sec',
                            grouping='time,dst_federation,dst_country', starttime=date_list[0][0], endtime=date_list[0][1])

            hs06sec = Grafana().get_data(hs06sec)

            pledges_sum = Query(agg_func='mean', table='pledges_hs06sec', field='value',
                                grouping='time,real_federation,country', starttime=date_list[0][0], endtime=date_list[0][1])
            pledges_sum = Grafana(database='monit_production_rebus').get_data(pledges_sum)
            pledges_dict = pledges_merging(hs06sec, pledges_sum, total_seconds,
                                                         pledges_dict, type='dst_country')
        for pledges in pledges_dict:
            if pledges == 'NULL':
                continue
            else:
                pledges_list.append({"dst_country":pledges, "hs06sec":int(round(float(pledges_dict[pledges]['hs06sec'])/86400, 2)),
                                      'pledges': int(round(float(pledges_dict[pledges]['pledges'])/86400, 2))})
        setCacheEntry(request, key, json.dumps(pledges_list),
                      60 * 60 * 24 * 30, isData=True)
        return HttpResponse(json.dumps(pledges_list), content_type='text/json')
    else:
        data = getCacheEntry(request, "pledges")
        #data = None
        if data is not None:
            data = json.loads(data)
            t = loader.get_template('grafana-pledges.html')
            return HttpResponse(t.render(data, request), content_type='text/html')
        else:
            key_fed = hashlib.md5(encoding.force_bytes("{0}_{1}_federation".format(starttime, endtime)))
            key_country = hashlib.md5(encoding.force_bytes("{0}_{1}_country".format(starttime, endtime)))
            key_fed = key_fed.hexdigest()
            key_country = key_country.hexdigest()
            setCacheEntry(request, key_fed, None, 60, isData=True)
            setCacheEntry(request, key_country, None, 60, isData=True)

        t = loader.get_template('grafana-pledges.html')
        data = {
            'request':request,
            'date_from': starttime,
            'date_to': endtime,
            'days': total_days,
            'info': "This page was cached: {0}".format(str(datetime.utcnow()))
        }
        setCacheEntry(request, "pledges", json.dumps(data, cls=DateEncoder), 60 * 60 * 24 * 30)
        return HttpResponse(t.render({"date_from":starttime, "date_to":endtime, "days":total_days}, request), content_type='text/html')