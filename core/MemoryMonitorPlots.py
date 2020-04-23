import pandas as pd
import io
import os
import urllib3
import logging
import math
from datetime import datetime
from core.common.models import Filestable4
from core.common.models import FilestableArch
import json
from django.http import HttpResponse
from django.views.decorators.cache import never_cache
import matplotlib.pyplot as plt
from PIL import Image
import numpy as np
from core.views import initRequest, login_customrequired
from django.shortcuts import render_to_response
import requests

from core.filebrowser.views import get_job_memory_monitor_output

_logger = logging.getLogger('bigpandamon')
filebrowserURL = "http://bigpanda.cern.ch/filebrowser/" #This is deployment specific because memory monitoring is intended to work in ATLAS


def collectData(pandaID):

    logFiles = []
    logFiles.extend(Filestable4.objects.filter(pandaid=pandaID, type='log').values())
    if len(logFiles) == 0:
        logFiles.extend(FilestableArch.objects.filter(pandaid=pandaID, type='log').values())
    if not len(logFiles) == 1:
        return HttpResponse('Log files for pandaid=%s not found'% pandaID)

    logfile = logFiles[0]
    guid = logfile['guid']
    lfn = logfile['lfn']
    scope = logfile['scope']
    http = urllib3.PoolManager()
    resp = http.request('GET', filebrowserURL, fields={'guid': guid, 'lfn': lfn, 'scope': scope, 'json':1})
    if resp and len(resp.data) > 0:
        try:
            data = json.loads(resp.data)
            HOSTNAME = data['HOSTNAME']
            tardir = data['tardir']
            MEDIA_URL = data['MEDIA_URL']
            dirprefix = data['dirprefix']
            files = data['files']
            files = [f for f in files if 'memory_monitor_output.txt' in f['name']]
        except:
            return -2
    else:
        return -2

    urlBase = "http://"+HOSTNAME+"/"+MEDIA_URL+"/"+dirprefix+"/"+tardir

    dfl = []
    pd.set_option('display.max_columns', 1000)
    for f in files:
        url = urlBase+f['dirname']+"/"+f['name']
        resp = requests.get(url, verify=False)
        if resp and len(resp.text) > 0:
            TESTDATA = io.StringIO(resp.text)
            dfl.append(pd.read_csv(TESTDATA, sep="\t").iloc[:])

            #TESTDATA = io.StringIO(str(html))
            #dfl.append(pd.read_csv(TESTDATA, sep="\t").iloc[:, range(9)])


    if len(dfl) > 0:
        df = pd.concat(dfl)
        column_names = sorted([cn.lower() for cn in list(df.columns)], key=lambda s: s.startswith('unnamed'))
        df.columns = column_names
        # df.columns = ['Time','VMEM','PSS','RSS','Swap','rchar','wchar','rbytes','wbytes']
        df = df.sort_values(by='time')
        tstart = df['time'].min()


        df['time'] = df['time'].apply(lambda x: x-tstart)
        memory_columns = ['pss', 'rss', 'vmem', 'swap', 'rchar', 'wchar', 'rbytes', 'wbytes']
        if 'read_bytes' in df.columns:
            df = df.rename(columns={'read_bytes': 'rbytes'})
        if 'write_bytes' in df.columns:
            df = df.rename(columns={'write_bytes': 'wbytes'})

        for mc in memory_columns:
            df[mc] = df[mc].apply(lambda x: x / 1024.0 / 1024.0)

        # Make plot for memory consumption
        f1 = plt.figure(figsize=(15, 10))
        ax1 = f1.add_subplot(111)
        ax1.plot(df['time'], df['pss'], label="PSS")
        ax1.legend(loc="upper right")

        ax2 = f1.add_subplot(111)
        ax2.plot(df['time'], df['rss'], label="RSS")
        ax2.legend(loc="upper right")

        ax3 = f1.add_subplot(111)
        ax3.plot(df['time'], df['swap'], label="Swap")
        ax3.legend(loc="upper right")

        ax4 = f1.add_subplot(111)
        ax4.plot(df['time'], df['vmem'], label="VMEM")
        ax4.legend(loc="upper right")

        plt.title("Memory consumption, job " + str(pandaID))
        plt.xlabel("time (s)")
        plt.ylabel("memory usage (GB)")
        plt.ylim(ymin=0)
        plt.xlim(xmin=0)
        plt.grid()

        minor_ticks = np.arange(0, plt.ylim()[1], 1) if plt.ylim()[1] > 1 else np.arange(0, plt.ylim()[1], 0.1)
        plt.minorticks_on()
        plt.yticks(minor_ticks)

        plot1img = io.BytesIO()
        plt.savefig(plot1img, format='png')
        plot1img.seek(0)


        #Make plot for IO
        f1 = plt.figure(figsize=(15, 10))
        ax1 = f1.add_subplot(111)
        ax1.plot(df['time'], df['rchar'], label="rchar")
        ax1.legend(loc="upper right")

        ax2 = f1.add_subplot(111)
        ax2.plot(df['time'], df['wchar'], label="wchar")
        ax2.legend(loc="upper right")

        ax3 = f1.add_subplot(111)
        ax3.plot(df['time'], df['rbytes'], label="rbytes")
        ax3.legend(loc="upper right")

        ax4 = f1.add_subplot(111)
        ax4.plot(df['time'], df['wbytes'], label="wbytes")
        ax4.legend(loc="upper right")

        plt.title("IO, job " + str(pandaID))
        plt.xlabel("time (s)")
        plt.ylabel("IO (MB)")
        plt.grid()
        plt.ylim(ymin=0)
        plt.xlim(xmin=0)

        plot2img = io.BytesIO()
        plt.savefig(plot2img, format='png')
        plot2img.seek(0)


        #Make plot for IO rate
        lasttime = 0
        lastrchar = 0
        lastwchar = 0
        lastrbytes = 0
        lastwbytes = 0

        drchar = [0]
        dwchar = [0]
        drbytes = [0]
        dwbytes = [0]

        for index, row in df.iterrows():
            if index > 0:
                dt = row['time']-lasttime
                drchar.append((row['rchar']-lastrchar)/dt)
                dwchar.append((row['wchar']-lastwchar)/dt)
                drbytes.append((row['rbytes']-lastrbytes)/dt)
                dwbytes.append((row['wbytes']-lastwbytes)/dt)
            lasttime = row['time']
            lastrchar = row['rchar']
            lastwchar = row['wchar']
            lastrbytes = row['rbytes']
            lastwbytes = row['wbytes']

        df['drchar'] = drchar
        df['dwchar'] = dwchar
        df['drbytes'] = drbytes
        df['dwbytes'] = dwbytes

        f1 = plt.figure(figsize=(15, 10))
        ax1 = f1.add_subplot(111)
        ax1.plot(df['time'], drchar, label="rchar")
        ax1.legend(loc="upper right")

        ax2 = f1.add_subplot(111)
        ax2.plot(df['time'], dwchar, label="wchar")
        ax2.legend(loc="upper right")

        ax3 = f1.add_subplot(111)
        ax3.plot(df['time'], drbytes, label="rbytes")
        ax3.legend(loc="upper right")

        ax4 = f1.add_subplot(111)
        ax4.plot(df['time'], dwbytes, label="wbytes")
        ax4.legend(loc="upper right")

        plt.title("IO rate, job " + str(pandaID))
        plt.xlabel("time (s)")
        plt.ylabel("IO rate (MB/S)")
        plt.grid()
        plt.ylim(ymin=0)
        plt.xlim(xmin=0)

        plot3img = io.BytesIO()
        plt.savefig(plot3img, format='png')
        plot3img.seek(0)

        #Here we combine few plots
        images = list(map(Image.open, [plot1img, plot2img, plot3img]))
        widths, heights = zip(*(i.size for i in images))
        max_width = max(widths)
        total_height = sum(heights)

        new_im = Image.new('RGB', (max_width, total_height))

        y_offset = 0
        for im in list(images):
            new_im.paste(im, (0, y_offset))
            y_offset += im.size[1]

        finPlotData = io.BytesIO()
        new_im.save(finPlotData, format='png')
        finPlotData.seek(0)

        if plot1img is not None:
            return HttpResponse(finPlotData.getvalue(), content_type="image/png")
    return HttpResponse('')



@never_cache
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

    return collectData(pandaid)


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
            msg = 'No memory monitor output file found for job'
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

