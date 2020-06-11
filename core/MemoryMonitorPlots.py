import pandas as pd
import os
import logging
import math
from datetime import datetime
import json
from django.http import HttpResponse
from django.views.decorators.cache import never_cache
from django.shortcuts import render_to_response, redirect

from core.views import initRequest, login_customrequired
from core.filebrowser.views import get_job_memory_monitor_output

_logger = logging.getLogger('bigpandamon')
filebrowserURL = "http://bigpanda.cern.ch/filebrowser/" #This is deployment specific because memory monitoring is intended to work in ATLAS


@login_customrequired
def getPlots(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    if not 'pandaid' in request.session['requestParams']:
        data = {
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            "errormessage": "No pandaid provided!",
        }
        return render_to_response('errorPage.html', data, content_type='text/html')
    else:
        pandaid = request.session['requestParams']['pandaid']
        try:
            pandaid = int(pandaid)
        except:
            data = {
                'viewParams': request.session['viewParams'],
                'requestParams': request.session['requestParams'],
                "errormessage": "Illegal value {} for pandaid provided! Check the URL please!".format(pandaid),
            }
            return render_to_response('errorPage.html', data, content_type='text/html')
    # redirect to new PrMon plots page
    return redirect('prMonPlots', pandaid=pandaid)


@login_customrequired
def prMonPlots(request, pandaid=-1):
    valid, response = initRequest(request)
    if not valid:
        return response

    msg = ''
    try:
        pandaid = int(pandaid)
    except:
        pandaid = -1
        msg = 'No pandaid provided!'

    plots_list = [
        'np_nt',
        'cpu_rate',
        'memory',
        'memory_rate',
        'io',
        'io_rate',
    ]

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'pandaid': pandaid,
        'built': datetime.now().strftime("%H:%M:%S"),
        'plotsList': plots_list,
        "error": msg,
    }
    return render_to_response('jobMemoryMonitor.html', data, content_type='text/html')


@never_cache
def getPrMonPlotsData(request, pandaid=-1):
    """
    Getting and prepare data for plots based on PRMON output data stored in job logs
    :param request:
    :param pandaid:
    :return: list of dicts containing data for plots
    """
    try:
        pandaid = int(pandaid)
    except:
        pandaid = -1

    # Definition of prmon labels/units for beautification from:
    # https://github.com/HSF/prmon/blob/master/package/scripts/prmon_plot.py
    axisunits = {'vmem': 'kb', 'pss': 'kb', 'rss': 'kb', 'swap': 'kb',
                 'utime': 'sec', 'stime': 'sec', 'wtime': 'sec',
                 'rchar': 'b', 'wchar': 'b',
                 'read_bytes': 'b', 'write_bytes': 'b',
                 'rx_packets': '1', 'tx_packets': '1',
                 'rx_bytes': 'b', 'tx_bytes': 'b',
                 'nprocs': '1', 'nthreads': '1'}

    axisnames = {'vmem': 'Memory',
                 'pss': 'Memory',
                 'rss': 'Memory',
                 'swap': 'Memory',
                 'utime': 'CPU-time',
                 'stime': 'CPU-time',
                 'wtime': 'Wall-time',
                 'rchar': 'I/O',
                 'wchar': 'I/O',
                 'read_bytes': 'I/O',
                 'write_bytes': 'I/O',
                 'rx_packets': 'Network',
                 'tx_packets': 'Network',
                 'rx_bytes': 'Network',
                 'tx_bytes': 'Network',
                 'nprocs': 'Count',
                 'nthreads': 'Count'}

    legendnames = {'vmem': 'Virtual Memory (VMEM)',
                   'pss': 'Proportional Set Size (PSS)',
                   'rss': 'Resident Set Size (RSS)',
                   'swap': 'Swap Size (SWAP)',
                   'utime': 'User CPU-time',
                   'stime': 'System CPU-time',
                   'wtime': 'Wall-time',
                   'rchar': 'I/O Read (rchar)',
                   'wchar': 'I/O Written (wchar)',
                   'read_bytes': 'I/O Read (read_bytes)',
                   'write_bytes': 'I/O Written (write_bytes)',
                   'rx_packets': 'Network Received (packets)',
                   'tx_packets': 'Network Transmitted (packets)',
                   'rx_bytes': 'Network Received (bytes)',
                   'tx_bytes': 'Network Transmitted (bytes)',
                   'nprocs': 'Number of Processes',
                   'nthreads': 'Number of Threads'}

    metric_groups = {
        'memory': ['vmem', 'pss', 'rss', 'swap'],
        'count': ['nprocs', 'nthreads'],
        'io': ['rchar', 'wchar', 'read_bytes', 'write_bytes'],
        'cpu': ['utime', 'stime'],
        'network': ['rx_bytes', 'tx_bytes'],
        'network_count': ['rx_packets', 'tx_packets'],
    }

    plots_details = {
        'np_nt': {'title': 'Number of processes and threads', 'ylabel': '', 'xlabel': 'Wall time, min'},
        'cpu_rate': {'title': 'CPU rate', 'ylabel': '', 'xlabel': 'Wall time, min'},
        'memory': {'title': 'Memory utilization', 'ylabel': 'Consumed memory, GB', 'xlabel': 'Wall time, min'},
        'memory_rate': {'title': 'Memory rate', 'ylabel': 'MB/min', 'xlabel': 'Wall time, min'},
        'io': {'title': 'I/O', 'ylabel': 'I/O, GB', 'xlabel': 'Wall time, min'},
        'io_rate': {'title': 'I/O rate', 'ylabel': 'MB/min', 'xlabel': 'Wall time, min'},
    }

    msg = ''
    plots_data = {}
    raw_data = pd.DataFrame()

    # get memory_monitor_output.txt file
    if pandaid > 0:
        mmo_path = get_job_memory_monitor_output(pandaid)

        # check if the file exists
        if mmo_path is not None and os.path.exists(mmo_path):
            # load the data from file
            raw_data = pd.read_csv(mmo_path, sep='\t')
        else:
            msg = """No memory monitor output file found in a job log tarball. 
                     It can happen if a job failed and logs were not saved 
                     or a life time of storing job logs are already expired."""
            _logger.warning(msg)

    # prepare data for plots
    if not raw_data.empty:
        raw_data['wtime_dt'] = raw_data['wtime'].diff()
        raw_data['wtime_min_dt'] = raw_data['wtime_dt']/60.

        for mi in metric_groups['io']:
            raw_data[mi + '_rate'] = raw_data[mi].diff()
            raw_data[mi + '_rate'] /= raw_data['wtime_min_dt']
            raw_data[mi + '_rate'] /= (1024.*1024.)
            raw_data[mi + '_rate'] = raw_data[mi + '_rate'].round(2)

        for mm in metric_groups['memory']:
            raw_data[mm + '_rate'] = raw_data[mm].diff()
            raw_data[mm + '_rate'] /= raw_data['wtime_min_dt']
            raw_data[mm + '_rate'] /= 1024.
            raw_data[mm + '_rate'] = raw_data[mm + '_rate'].round(3)

        for mc in metric_groups['cpu']:
            raw_data[mc + '_rate'] = raw_data[mc].diff()
            raw_data[mc + '_rate'] /= raw_data['wtime_dt']
            raw_data[mc + '_rate'] = raw_data[mc + '_rate'].round(3)

        for mm in metric_groups['memory']:
            raw_data[mm] = raw_data[mm]/1024./1024.
            raw_data[mm] = raw_data[mm].round(2)

        for mi in metric_groups['io']:
            raw_data[mi] = raw_data[mi]/1024./1024./1024.
            raw_data[mi] = raw_data[mi].round(2)

        # if number of data points too high we interpolate values on reduced wtime interval frequency
        N_MAX = 600.
        if len(raw_data['wtime']) > N_MAX:
            new_data = raw_data.copy()
            new_step = math.floor((new_data['wtime'][2] - new_data['wtime'][1])*len(new_data['wtime'])/N_MAX)
            inter_index = pd.RangeIndex(start=new_data['wtime'].min(), stop=new_data['wtime'].max(), step=new_step)
            new_data.set_index('wtime', inplace=True)
            raw_data = new_data.reindex(inter_index, method='nearest').interpolate()
            raw_data['wtime'] = raw_data.index.values

        # replace NaN values by 0
        raw_data = raw_data.fillna(0)
        raw_data['wtime_min'] = raw_data['wtime']/60
        raw_data['wtime_min'] = raw_data['wtime_min'].round(0)

        for pname, pdet in plots_details.items():
            plots_data[pname] = {'data': [['x']], 'details': pdet}
            plots_data[pname]['data'][0].extend(raw_data['wtime_min'].tolist())

        for mc in metric_groups['count']:
            tmp = [legendnames[mc]]
            tmp.extend(raw_data[mc].tolist())
            plots_data['np_nt']['data'].append(tmp)

        for mc in metric_groups['cpu']:
            tmp = [legendnames[mc]]
            tmp.extend(raw_data[mc + '_rate'].tolist())
            plots_data['cpu_rate']['data'].append(tmp)

        for mm in metric_groups['memory']:
            tmp = [legendnames[mm]]
            tmp.extend(raw_data[mm].tolist())
            plots_data['memory']['data'].append(tmp)

            is_cut_rate_plot_data = False
            if tmp[-1] == 0:
                is_cut_rate_plot_data = True

            tmp = [legendnames[mm]]
            tmp.extend(raw_data[mm + '_rate'].tolist())
            if is_cut_rate_plot_data:
                tmp = tmp[:-1]
            plots_data['memory_rate']['data'].append(tmp)

        for mi in metric_groups['io']:
            tmp = [legendnames[mi]]
            tmp.extend(raw_data[mi].tolist())
            plots_data['io']['data'].append(tmp)

            is_cut_rate_plot_data = False
            if tmp[-1] == 0:
                is_cut_rate_plot_data = True

            tmp = [legendnames[mi]]
            tmp.extend(raw_data[mi + '_rate'].tolist())
            if is_cut_rate_plot_data:
                tmp = tmp[:-1]
            plots_data['io_rate']['data'].append(tmp)

    data = {
        'plotsDict': plots_data,
        'error': msg,
    }

    return HttpResponse(json.dumps(data), content_type='application/json')

