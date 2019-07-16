import pandas as pd
import io
import urllib3
from core.common.models import Filestable4
from core.common.models import FilestableArch
import json
from django.http import HttpResponse
from django.views.decorators.cache import never_cache
import matplotlib.pyplot as plt
from PIL import Image
import numpy as np
from core.views import initRequest
from django.shortcuts import render_to_response
import requests

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


