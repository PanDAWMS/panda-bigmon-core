"""
A set of utilities to build jobs resource utilization plots
"""

import time
import logging
from sys import getsizeof
from core.libs.exlib import convert_bytes, build_stack_histogram
from core.libs.job import get_job_walltime, get_job_queuetime, parse_job_pilottiming


_logger = logging.getLogger('bigpandamon')


def job_consumption_plots(jobs):
    """
    Prepare data to plot stack histograms of different job metrics. Expected job fields:
        computingsite, status, category, corecount, leak,
        nevents, maxpss, cpuconsumptiontime, workdirsize, dbtime, pilottiming, dbdata
        creationdate, starttime, endtime
    :param jobs: list of dicts
    :return:
    """
    start_time = time.time()
    plots_dict = {}

    plot_templates = {
        'nevents_sum': {
            'per': [''],
            'jobstatus': ['finished'],
            'type': 'pie',
            'group_by': 'computingsite',
            'title': 'Number of events',
            'xlabel': 'N events',
        },
        'nevents': {
            'per': [''],
            'jobstatus': ['finished'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Number of events',
            'xlabel': 'N events',
        },
        'resimevents': {
            'per': [''],
            'jobstatus': ['finished'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Resim events (finished jobs)',
            'xlabel': 'N resim events',
        },
        'maxpss': {
            'per': ['', 'percore'],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Max PSS',
            'xlabel': 'MaxPSS, MB',
        },
        'queuetime': {
            'per': ['', ],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Time to start',
            'xlabel': 'Time to start, s',
        },
        'walltime': {
            'per': ['', 'perevent'],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Walltime',
            'xlabel': 'Walltime, s',
        },
        'hs06s': {
            'per': ['', 'perevent'],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'HS06s',
            'xlabel': 'HS06s',
        },
        'cputime': {
            'per': ['', 'perevent'],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'CPU time',
            'xlabel': 'CPU time, s',
        },
        'cpuefficiency': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'CPU efficiency',
            'xlabel': 'CPU efficiency, %',
        },
        'dbtime': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'DB time',
            'xlabel': 'DB time, s',
        },
        'timegetjob': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Fetch job time',
            'xlabel': 'Time, s',
        },
        'timestagein': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Stagein time',
            'xlabel': 'Time, s',
        },
        'timepayload': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Payload running time',
            'xlabel': 'Time, s',
        },
        'timestageout': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Stage out time',
            'xlabel': 'Time, s',
        },
        'timetotal_setup': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Setup time',
            'xlabel': 'Time, s',
        },
        'dbdata': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'DB data',
            'xlabel': 'DB data, MB',
        },
        'workdirsize': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Workdir size',
            'xlabel': 'Workdir, MB',
        },
        'leak': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'Memory leak',
            'xlabel': 'Memory leak, B/s',
        },
        'nprocesses': {
            'per': [''],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'computingsite',
            'title': 'N processes',
            'xlabel': 'N processes',
        },
        'walltime_bycpuunit': {
            'per': ['', ],
            'jobstatus': ['finished', 'failed'],
            'type': 'stack_bar',
            'group_by': 'cpuconsumptionunit',
            'title': 'Walltime',
            'xlabel': 'Walltime, s',
        },
    }

    plot_details = {}
    for pname, ptemp in plot_templates.items():
        for per in ptemp['per']:
            for js in ptemp['jobstatus']:
                plot_details[pname + per + '_' + js] = {
                    'type': ptemp['type'],
                    'group_by': ptemp['group_by'],
                    'title': ptemp['title'] + per.replace('per', ' per ') + ' ({} jobs)'.format(js),
                    'xlabel': ptemp['xlabel'],
                    'ylabel': 'N jobs',
                }

    plots_data = {}
    for pname, pd in plot_details.items():
        if pd['type'] not in plots_data:
            plots_data[pd['type']] = {}
        plots_data[pd['type']][pname] = {
            'build': {},
            'run': {},
            'merge': {}
        }

    MULTIPLIERS = {
        "SEC": 1.0,
        "MIN": 60.0,
        "HOUR": 60.0 * 60.0,
        "MB": 1024.0,
        "GB": 1024.0 * 1024.0,
    }

    # prepare data for plots
    for job in jobs:
        if job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if 'duration' not in job:
            job['duration'] = get_job_walltime(job)
        if 'queuetime' not in job:
            job['queuetime'] = get_job_queuetime(job)
        # protection if cpuconsumptiontime is decimal in non Oracle DBs
        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None:
            job['cpuconsumptiontime'] = float(job['cpuconsumptiontime'])
        if 'pilottiming' in job:
            job.update(parse_job_pilottiming(job['pilottiming']))

        if job['jobstatus'] in ('finished', 'failed'):
            for pname, pd in plot_details.items():
                if pd['group_by'] in job and job[pd['group_by']] not in plots_data[pd['type']][pname][job['category']]:
                    plots_data[pd['type']][pname][job['category']][job[pd['group_by']]] = []
        else:
            continue

        if 'nevents' in job and job['nevents'] > 0 and job['jobstatus'] == 'finished':
            plots_data['stack_bar']['nevents' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['nevents'])

            plots_data['pie']['nevents_sum_finished'][job['category']][job['computingsite']].append(job['nevents'])

        if 'maxpss' in job and job['maxpss'] is not None and job['maxpss'] >= 0:
            plots_data['stack_bar']['maxpss' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['maxpss'] / MULTIPLIERS['MB']
            )
            if job['actualcorecount'] and job['actualcorecount'] > 0:
                plots_data['stack_bar']['maxpsspercore' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['maxpss'] / MULTIPLIERS['MB'] / job['actualcorecount']
                )

        if 'hs06sec' in job and job['hs06sec']:
            plots_data['stack_bar']['hs06s' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['hs06sec'])

            if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['hs06sperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['hs06sec'] / (job['nevents'] * 1.0)
                )

        if 'queuetime' in job and job['queuetime']:
            plots_data['stack_bar']['queuetime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['queuetime'])

        if 'duration' in job and job['duration']:
            plots_data['stack_bar']['walltime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['duration'])
            if 'walltimeperevent' in job:
                plots_data['stack_bar']['walltimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['walltimeperevent']
                )
            elif 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['walltimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['duration'] / (job['nevents'] * 1.0)
                )

            if 'cpuconsumptionunit' in job and job['cpuconsumptionunit']:
                plots_data['stack_bar']['walltime_bycpuunit' + '_' + job['jobstatus']][job['category']][job['cpuconsumptionunit']].append(job['duration'])

        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None and job['cpuconsumptiontime'] > 0:
            plots_data['stack_bar']['cputime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['cpuconsumptiontime']
            )
            if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['cputimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['cpuconsumptiontime'] / (job['nevents'] * 1.0)
                )
            if 'actualcorecount' in job and job['actualcorecount'] > 0 and 'duration' in job and job['duration'] > 0:
                plots_data['stack_bar']['cpuefficiency' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    100.0 * job['cpuconsumptiontime']/job['duration']/job['actualcorecount'])

        if 'leak' in job and job['leak'] is not None:
            plots_data['stack_bar']['leak' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['leak']
            )
        if 'nprocesses' in job and job['nprocesses'] is not None and job['nprocesses'] > 0:
            plots_data['stack_bar']['nprocesses' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['nprocesses']
            )
        if 'workdirsize' in job and job['workdirsize'] is not None and job['workdirsize'] > 0:
            plots_data['stack_bar']['workdirsize' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                convert_bytes(job['workdirsize'], output_unit='MB')
            )
        if 'dbtime' in job and job['dbtime'] is not None and job['dbtime'] > 0:
            plots_data['stack_bar']['dbtime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['dbtime']
            )
        if 'dbdata' in job and job['dbdata'] is not None and job['dbdata'] > 0:
            plots_data['stack_bar']['dbdata' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                convert_bytes(job['dbdata'], output_unit='MB')
            )
        if 'timegetjob' in job and job['timegetjob'] is not None and job['timegetjob'] > 0:
            plots_data['stack_bar']['timegetjob' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['timegetjob']
            )
        if 'timestagein' in job and job['timestagein'] is not None and job['timestagein'] > 0:
            plots_data['stack_bar']['timestagein' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['timestagein']
            )
        if 'timepayload' in job and job['timepayload'] is not None and job['timepayload'] > 0:
            plots_data['stack_bar']['timepayload' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['timepayload']
            )
        if 'timestageout' in job and job['timestageout'] is not None and job['timestageout'] > 0:
            plots_data['stack_bar']['timestageout' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['timestageout']
            )
        if 'timetotal_setup' in job and job['timetotal_setup'] is not None and job['timetotal_setup'] > 0:
            plots_data['stack_bar']['timetotal_setup' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['timetotal_setup']
            )

        if 'resimevents' in job and job['resimevents'] and job['jobstatus'] == 'finished':
            plots_data['stack_bar']['resimevents' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['resimevents'])

    _logger.info("prepare plots data: {} sec".format(time.time() - start_time))

    # remove empty categories
    cat_to_remove = {'build': True, 'run': True, 'merge': True}
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            for cat, cd in pd.items():
                if len(cd) > 0:
                    cat_to_remove[cat] = False
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            for cat, is_remove in cat_to_remove.items():
                if is_remove:
                    del pd[cat]

    # add 'all' category to histograms
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            all_cat = {}
            for cat, cd in pd.items():
                for site, sd in cd.items():
                    if site not in all_cat:
                        all_cat[site] = []
                    all_cat[site].extend(sd)
            pd['all'] = all_cat

    # remove empty plots
    plots_to_remove = []
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            if sum([len(site_data) for site, site_data in pd['all'].items()]) == 0:
                plots_to_remove.append(pm)
    for pm in plots_to_remove:
        for pt, td in plots_data.items():
            if pm in td:
                del plots_data[pt][pm]
                del plot_details[pm]
    _logger.info("clean up plots data: {} sec".format(time.time() - start_time))

    # prepare stack histogram data
    for pname, pd in plot_details.items():
        if pd['type'] == 'stack_bar':
            plots_dict[pname] = {
                'details': plot_details[pname],
                'data': {},
            }

            for cat, cd in plots_data[pd['type']][pname].items():
                n_decimals = 1
                if 'per' in pname:
                    n_decimals = 2
                stats, columns = build_stack_histogram(cd, n_decimals=n_decimals)
                plots_dict[pname]['data'][cat] = {
                    'columns': columns,
                    'stats': stats,
                }
        elif pd['type'] == 'pie':
            plots_dict[pname] = {
                'details': plot_details[pname],
                'data': {},
            }
            for cat, cd in plots_data[pd['type']][pname].items():

                columns = []
                for site in cd:
                    columns.append([site, sum(cd[site])])

                plots_dict[pname]['data'][cat] = {
                    'columns': sorted(columns, key=lambda x: -x[1]),
                }
            if max([len(i['columns']) for i in plots_dict[pname]['data'].values()]) > 15:
                plots_dict[pname]['details']['legend_position'] = 'bottom'
                plots_dict[pname]['details']['size'] = [800, 300 + 20 * int(max([len(i['columns']) for i in plots_dict[pname]['data'].values()])/6)]
    _logger.info("built plots: {} sec".format(time.time() - start_time))

    # transform dict to list
    plots_list = []
    for pname, pdata in plots_dict.items():
        plots_list.append({'name': pname, 'data': pdata})

    # sort finished then failed
    plots_list = sorted(plots_list, key=lambda x: x['name'].split('_')[-1], reverse=True)

    return plots_list
