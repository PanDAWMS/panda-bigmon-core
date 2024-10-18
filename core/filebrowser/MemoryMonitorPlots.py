import pandas as pd
import os
import logging
import math
import re
from datetime import datetime
import json
from django.http import JsonResponse
from django.views.decorators.cache import never_cache
from django.shortcuts import render, redirect

from core.oauth.utils import login_customrequired
from core.views import initRequest
from core.utils import error_response
from core.libs.datetimestrings import parse_datetime
from core.libs.job import get_job_list
from core.filebrowser.utils import get_job_log_file_path

from django.conf import settings

_logger = logging.getLogger('bigpandamon-filebrowser')


@login_customrequired
def getPlots(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    if not 'pandaid' in request.session['requestParams']:
        return error_response(request, message='No pandaid provided!', status=400)
    else:
        try:
            pandaid = int(request.session['requestParams']['pandaid'])
        except ValueError:
            return error_response(request, message='Illegal value for pandaid, it must be a number!', status=400)

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

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'pandaid': pandaid,
        'built': datetime.now().strftime("%H:%M:%S"),
        "error": msg,
    }
    return render(request, 'jobMemoryMonitor.html', data, content_type='text/html')


def get_seconds(line):
    """gets the seconds from timestamp in athena output (location of timestamp is hard-coded)"""
    matches = re.findall(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}', line)
    if len(matches) > 0:
        t = parse_datetime(matches[0])
        seconds = (t-datetime(1970, 1, 1)).total_seconds()  # get seconds
    else:
        seconds = None
    return seconds


def is_cut_last_points(data):
    """
    Decide if the last point(s) of the rate plots needs to be removed for better representation
    :param data: list of y values
    :return: bool
    """
    if data[-1] == 0:
        return True
    else:
        return False



def get_payload_steps(payload_stdout_path):
    """
    Read payload.stdout log to find name and start time in min of each step
    :param payload_stdout_path: str - path to payload.stdout log
    :return: payload_steps: list
    """
    payload_steps = []

    init_time = True
    with open(payload_stdout_path) as f:
        for line in f:
            if "INFO" in line and init_time:
                init_time = False
                firsttime = get_seconds(line)

            if "Starting execution" in line:
                try:
                    payload_steps.append([math.floor((get_seconds(line) - firsttime)/60), str(line.split()[7])])
                except Exception as e:
                    _logger.debug('Failed to get timestamp from log line: {}\n{}'.format(line, e))
            if "INFO Validating output files" in line:
                try:
                    payload_steps.append([math.floor((get_seconds(line) - firsttime) / 60), "VALIDATION"])
                except Exception as e:
                    _logger.debug('Failed to get timestamp from log line: {}\n{}'.format(line, e))

    # remove following steps if they can overlap
    distances = [i_step[0] - payload_steps[i-1][0] for i, i_step in enumerate(payload_steps) if i > 0]
    if len([i for i, d in enumerate(distances) if d < 1]) > 0:
        for j in sorted([i for i, d in enumerate(distances) if d < 1], reverse=True):
            try:
                del payload_steps[j+1]
                payload_steps[j][1] += '[+]'
            except:
                pass
    return payload_steps


@never_cache
def getPrMonPlotsData(request, pandaid=-1):
    """
    Getting and prepare data for plots based on PRMON output data stored in job logs
    :param request:
    :param pandaid:
    :return: list of dicts containing data for plots
    """
    valid, response = initRequest(request)
    if not valid:
        return response
    try:
        pandaid = int(pandaid)
    except:
        pandaid = -1

    # Definition of prmon labels/units for beautification from:
    # https://github.com/HSF/prmon/blob/master/package/scripts/prmon_plot.py
    legendnames = {
        'vmem': 'Virtual Memory (VMEM)',
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
        'nthreads': 'Number of Threads',
        'gpufbmem': 'GPU Memory',
        'gpumempct': 'GPU Memory',
        'gpusmpct': 'GPU Streaming Multiprocessors',
        'ngpus': 'Number of GPUs'
    }

    metric_groups = {
        'memory': ['vmem', 'pss', 'rss', 'swap'],
        'count': ['nprocs', 'nthreads'],
        'io': ['rchar', 'wchar', 'read_bytes', 'write_bytes'],
        'cpu': ['utime', 'stime'],
        'network': ['rx_bytes', 'tx_bytes'],
        'network_count': ['rx_packets', 'tx_packets'],
        'gpu_count': ['ngpus'],
        'gpu_memory': ['gpufbmem'],
        'gpu_pct': ['gpumempct', 'gpusmpct'],
    }

    plots_details = {
        'np_nt': {'title': 'Number of processes and threads', 'ylabel': '', 'xlabel': 'Wall time, min'},
        'cpu_rate': {'title': 'CPU rate', 'ylabel': '', 'xlabel': 'Wall time, min'},
        'memory': {'title': 'Memory utilization', 'ylabel': 'Consumed memory, GB', 'xlabel': 'Wall time, min'},
        'memory_rate': {'title': 'Memory rate', 'ylabel': 'MB/min', 'xlabel': 'Wall time, min'},
        'io': {'title': 'I/O', 'ylabel': 'I/O, GB', 'xlabel': 'Wall time, min'},
        'io_rate': {'title': 'I/O rate', 'ylabel': 'MB/min', 'xlabel': 'Wall time, min'},
        'ng': {'title': 'Number of GPUs', 'ylabel': '', 'xlabel': 'Wall time, min'},
        'gpu_memory': {'title': 'GPU memory', 'ylabel': 'Consumed memory, GB', 'xlabel': 'Wall time, min'},
        'gpu_memory_rate': {'title': 'GPU memory rate', 'ylabel': 'MB/min', 'xlabel': 'Wall time, min'},
        'gpu_res': {'title': 'GPU utilization percentage', 'ylabel': '%', 'xlabel': 'Wall time, min'},
        'memory_fit': {'title': 'Memory utilization fitting results', 'ylabel': 'GB', 'xlabel': 'Wall time, min'},
    }

    msg = ''
    plots_data = {}
    raw_data = pd.DataFrame()
    sum_data = {}
    payload_steps = []

    # get memory_monitor_output.txt file
    if pandaid > 0:
        mmo_path = get_job_log_file_path(pandaid, 'memory_monitor_output.txt')
        # check if the file exists
        if mmo_path is not None and (os.path.exists(mmo_path) or settings.PRMON_LOGS_DIRECTIO_LOCATION):
            # load the data from file
            try:
                raw_data = pd.read_csv(mmo_path, delim_whitespace=True)
            except Exception as ex:
                _logger.exception('Failed to open memory output file with {}'.format(ex))
            # get memory_monitor_summary.json
            mms_path = get_job_log_file_path(pandaid, 'memory_monitor_summary.json')
            if mms_path is not None and os.path.exists(mms_path):
                with open(mms_path) as json_file:
                    try:
                        sum_data = json.load(json_file)
                    except Exception as e:
                        _logger.exception('Failed to load json from memory_monitor_summary.json file\n{}'.format(e))
            if 'ATLAS' in settings.DEPLOYMENT:
                # get payload steps
                pso_path = get_job_log_file_path(pandaid, 'payload.stdout')
                if pso_path is not None and os.path.exists(pso_path):
                    try:
                        payload_steps = get_payload_steps(pso_path)
                    except Exception as e:
                        _logger.exception("Error in getting athena info\n{}".format(e))
        else:
            msg = """No memory monitor output file found in a job log tarball. 
                     It can happen if a job failed and logs were not saved 
                     or a life time of storing job logs are already expired."""
            _logger.warning(msg)
    else:
        return error_response(request, message='No pandaid provided!', status=400)

    # rename columns if old memory monitor is used
    if not raw_data.empty:
        raw_data = raw_data.rename(str.lower, axis='columns')
        raw_data = raw_data.rename(columns={'rbytes': 'read_bytes', 'wbytes': 'write_bytes'})
        if 'time' in raw_data.columns:
            tstart = raw_data['time'].min()
            raw_data['wtime'] = raw_data['time'].apply(lambda x: x - tstart)

    # prepare data for plots
    if not raw_data.empty:
        raw_data['wtime_dt'] = raw_data['wtime'].diff()
        raw_data['wtime_min_dt'] = raw_data['wtime_dt']/60.

        # update metrics groups depending on available metrics in memory output file
        for mgn, mgl in metric_groups.items():
            metric_groups[mgn] = list(set(mgl) & set(raw_data.columns))

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

        for mm in metric_groups['gpu_memory']:
            raw_data[mm + '_rate'] = raw_data[mm].diff()
            raw_data[mm + '_rate'] /= raw_data['wtime_min_dt']
            raw_data[mm + '_rate'] /= 1024.
            raw_data[mm + '_rate'] = raw_data[mm + '_rate'].round(3)
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
        raw_data['wtime_min'] = raw_data['wtime_min'].round(1)

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

            tmp = [legendnames[mm]]
            tmp.extend(raw_data[mm + '_rate'].tolist())
            if is_cut_last_points(tmp):
                tmp = tmp[:-1]
            plots_data['memory_rate']['data'].append(tmp)

        for mi in metric_groups['io']:
            tmp = [legendnames[mi]]
            tmp.extend(raw_data[mi].tolist())
            plots_data['io']['data'].append(tmp)

            tmp = [legendnames[mi]]
            tmp.extend(raw_data[mi + '_rate'].tolist())
            if is_cut_last_points(tmp):
                tmp = tmp[:-1]
            plots_data['io_rate']['data'].append(tmp)

        for mm in metric_groups['gpu_memory']:
            tmp = [legendnames[mm]]
            tmp.extend(raw_data[mm].tolist())
            plots_data['gpu_memory']['data'].append(tmp)

            tmp = [legendnames[mm]]
            tmp.extend(raw_data[mm + '_rate'].tolist())
            if is_cut_last_points(tmp):
                tmp = tmp[:-1]
            plots_data['gpu_memory_rate']['data'].append(tmp)

        for mc in metric_groups['gpu_count']:
            tmp = [legendnames[mc]]
            tmp.extend(raw_data[mc].tolist())
            plots_data['ng']['data'].append(tmp)

        for mc in metric_groups['gpu_pct']:
            tmp = [legendnames[mc]]
            tmp.extend(raw_data[mc].tolist())
            plots_data['gpu_res']['data'].append(tmp)

        # get memory leak reported by pilot from job records & add memory fit plot
        jobs = get_job_list({"pandaid": pandaid}, values=['memoryleak', 'memoryleakx2'])
        if len(jobs) > 0:
            job_memory_leak = jobs[0].get('memoryleak', None)  # memory leak in KB/s
            job_memory_leak_x2 = jobs[0].get('memoryleakx2', None)  # memory leak square statistic
            plots_details['memory_fit']['title'] += f' (leak={job_memory_leak}KB/s; \u03c7\u00b2={job_memory_leak_x2}) '
            if job_memory_leak is not None and job_memory_leak > 0:
                # we plot memory fit i.e. pss+swap & memory leak slope i.e. a for y=ax+b
                job_memory_leak_gb_per_min = job_memory_leak * 60. / 1000. / 1000.  # transform KB/s to MB/min
                plots_data['memory_fit'] = {'data': [['x']], 'details': plots_details['memory_fit']}
                plots_data['memory_fit']['data'][0].extend(raw_data['wtime_min'].tolist())
                plots_data['memory_fit']['data'].append(
                    ['PSS+SWAP', ] + [sum(x) for x in zip(raw_data['pss'].tolist(), raw_data['swap'].tolist())])
                # find b for y=ax+b, depending on the number of available data points, take average of +-X points
                if len(raw_data['wtime_min']) >= 100:
                    range_points = [2, 10]
                elif 10 < len(raw_data['wtime_min']) < 100:
                    range_points = [2, 5]
                else:
                    # take first point
                    range_points = [1, 2]
                b = sum(plots_data['memory_fit']['data'][1][range_points[0]:range_points[1]])/(range_points[1]-range_points[0])
                plots_data['memory_fit']['data'].append(
                    ['Fitted memory utilization', ] +
                    [round(b + x * job_memory_leak_gb_per_min, 2) for x in raw_data['wtime_min'].tolist()])

        # set ymax for plots
        for pname, pdet in plots_details.items():
            plots_data[pname]['details']['ymax'] = 0
            for row in plots_data[pname]['data'][1:]:
                if len(row[1:]) > 1 and max(row[1:]) + abs(min(row[1:])) / 2 > plots_data[pname]['details']['ymax']:
                    plots_data[pname]['details']['ymax'] = max(row[1:]) + abs(min(row[1:])) / 2

        # add grid lines as payload steps extracted from stdout and adjust ymax
        if len(payload_steps) > 0:
            for pname, pdet in plots_details.items():
                plots_data[pname]['grid'] = {'x': {'lines': []}}
                for step in payload_steps:
                    plots_data[pname]['grid']['x']['lines'].append({'value': step[0], 'text': step[1]})
                # adjust y max
                plots_data[pname]['details']['ymax'] *= 1.35

    # remove plot if no data
    remove_list = []
    for pn, pdata in plots_data.items():
        if len(pdata['data']) <= 1:
            remove_list.append(pn)
    for i in remove_list:
        del plots_data[i]

    # prepare HW data from memory monitor summary
    hw_info = []
    if len(sum_data) > 0 and 'HW' in sum_data:
        if 'cpu' in sum_data['HW']:
            tmp_dict = {'type': 'CPU', 'str': ''}
            tmp_dict['str'] += sum_data['HW']['cpu']['ModelName'] + ', ' if 'ModelName' in sum_data['HW']['cpu'] else ''
            tmp_dict['str'] += '{} cores, '.format(sum_data['HW']['cpu']['CPUs']) if 'CPUs' in sum_data['HW']['cpu'] else ''
            tmp_dict['str'] += '{} sockets, '.format(sum_data['HW']['cpu']['Sockets']) if 'Sockets' in sum_data['HW']['cpu'] else ''
            tmp_dict['str'] += '{} cores/socket, '.format(sum_data['HW']['cpu']['CoresPerSocket']) if 'CoresPerSocket' in sum_data['HW']['cpu'] else ''
            tmp_dict['str'] += '{} threads/core, '.format(sum_data['HW']['cpu']['ThreadsPerCore']) if 'ThreadsPerCore' in sum_data['HW']['cpu'] else ''
            if 'mem' in sum_data['HW'] and 'MemTotal' in sum_data['HW']['mem']:
                tmp_dict['str'] += '{}GB of memory in total, '.format(round(sum_data['HW']['mem']['MemTotal']/1024./1024., 2)) if isinstance(sum_data['HW']['mem']['MemTotal'], int) else ''
            tmp_dict['str'] = tmp_dict['str'][:-2] if tmp_dict['str'].endswith(', ') else tmp_dict['str']
            hw_info.append(tmp_dict)
        if 'gpu' in sum_data['HW']:
            for gpu, info in sum_data['HW']['gpu'].items():
                if gpu != 'nGPU':
                    tmp_dict = {'type': 'GPU', 'str': ''}
                    tmp_dict['str'] += info['name'] + ', ' if 'name' in info else ''
                    tmp_dict['str'] += '{}MHz of processor core clock, '.format(info['sm_freq']) if 'sm_freq' in info else ''
                    tmp_dict['str'] += '{}GB, '.format(round(info['total_mem']/1024./1024., 2)) if 'total_mem' in info else ''
                    tmp_dict['str'] = tmp_dict['str'][:-2] if tmp_dict['str'].endswith(', ') else tmp_dict['str']
                    hw_info.append(tmp_dict)
        # sort HW info list
        hw_info = sorted(hw_info, key=lambda x: x['type'])

    # extraction prmon info, e.g. version
    prmon_info = ''
    if len(sum_data) > 0 and 'prmon' in sum_data:
        for k in sum_data['prmon']:
            prmon_info += ', {} {}'.format(k.lower(), str(sum_data['prmon'][k]))

    data = {
        'plotsDict': plots_data,
        'hwInfo': hw_info,
        'prmonInfo': prmon_info,
        'error': msg,
    }
    return JsonResponse(data, content_type='application/json')
