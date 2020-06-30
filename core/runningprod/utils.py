"""
Created by Tatiana Korchuganova on 01.10.2019
Utils for runningProdTasks module
"""

import numpy as np
from datetime import datetime
from django.db import transaction, DatabaseError
from core.runningprod.models import ProdNeventsHistory


def clean_running_task_list(task_list):
    """
    Cleaning task list
    :param task_list: list of dicts
    :return:
    """
    for task in task_list:
        task['rjobs'] = 0 if task['rjobs'] is None else task['rjobs']
        task['percentage'] = 0 if task['percentage'] is None else round(100 * task['percentage'], 1)
        task['nevents'] = task['nevents'] if task['nevents'] is not None else 0
        task['neventsused'] = task['neventsused'] if 'neventsused' in task and task['neventsused'] is not None else 0
        task['neventstobeused'] = task['neventstobeused'] if 'neventstobeused' in task and task['neventstobeused'] is not None else 0
        task['neventsrunning'] = task['neventsrunning'] if 'neventsrunning' in task and task['neventsrunning'] is not None else 0
        task['neventswaiting'] = task['neventstobeused']
        task['neventsdone'] = task['neventsused']
        task['slots'] = task['slots'] if task['slots'] else 0
        task['aslots'] = task['aslots'] if task['aslots'] else 0

        if task['corecount'] == 1:
            task['rjobs_score'] = task['rjobs']
        if task['corecount'] == 8:
            task['rjobs_mcore'] = task['rjobs']

        task['age'] = round((datetime.now() - task['creationdate']).total_seconds() / 3600. / 24., 1)

        if len(task['campaign'].split(':')) > 1:
            task['cutcampaign'] = task['campaign'].split(':')[1]
        else:
            task['cutcampaign'] = task['campaign'].split(':')[0]
        if 'reqid' in task and 'jeditaskid' in task and task['reqid'] == task['jeditaskid']:
            task['reqid'] = None
        if 'runnumber' in task:
            task['inputdataset'] = int(task['runnumber'])
        else:
            task['inputdataset'] = None

        task['outputtypes'] = ''
        if 'outputdatasettype' in task:
            outputtypes = task['outputdatasettype'].split(',')
        else:
            outputtypes = []
        if len(outputtypes) > 0:
            clean_outputtypes = []
            for outputtype in outputtypes:
                task['outputtypes'] += outputtype.split('_')[1] + ' ' if '_' in outputtype else ''
                cot = outputtype
                if '_' in outputtype:
                    cot = outputtype.split('_')[1]
                if not cot.startswith('PHYS'):
                    cot = cot[:4]
                clean_outputtypes.append(cot)
            clean_outputtypes = list(set(clean_outputtypes))
            task['outputdatatype'] = clean_outputtypes[0] if len(clean_outputtypes) == 1 else ' '.join(clean_outputtypes)

        if 'hashtags' in task and len(task['hashtags']) > 1:
            task['hashtaglist'] = []
            for hashtag in task['hashtags'].split(','):
                task['hashtaglist'].append(hashtag)

    return task_list


def prepare_plots(task_list, productiontype=''):
    """
    Prepare all necessary plot data
    :param task_list: list of dict
    :param productiontype: str
    :return:
    """
    plots_groups = {
        'main': ['nevents_sum_status', 'nevents_by_processingtype', 'aslots_by_processingtype', 'age_hist'],
        'main_preset': [],
        'extra': []
    }
    plots_dict = {
        'nevents_sum_status': {
            'data': [],
            'title': 'Event status',
            'options': {}
        },
        'nevents_by_status': {
            'data': [],
            'title': 'Task status',
            'options': {}
        },
        'nevents_by_priority': {
            'data': [],
            'title': 'Task priority',
            'options': {}
        },
        'nevents_by_processingtype': {
            'data': [],
            'title': 'Processing type',
            'options': {}
        },
        'aslots_by_processingtype': {
            'data': [],
            'title': 'Processing type',
            'options': {}
        },
        'age_hist': {
            'data': [],
            'title': 'Task age histogram',
            'options': {'labels': ['Task age, days', 'Number of tasks']}
        },
    }

    plots_data = {
        'group_by': {
            'nevents': {
                'processingtype': {},
                'status': {},
                'priority': {},
            },
            'aslots': {
                'processingtype': {}
            },
        },
        'hist': {
            'age': {
                'stats': {},
                'rawdata': [],
            },
        },
        'sum': {
            'nevents': {
                'status': {
                    'waiting': 0,
                    'running': 0,
                    'done': 0,
                }
            }
        },
    }

    if productiontype == 'MC':
        plots_data['group_by']['nevents']['simtype'] = {}
        plots_data['group_by_by'] = {
            'nevents': {
                'simtype_processingtype': {}
            }
        }
        plots_dict['nevents_by_simtype'] = {
            'data': [],
            'title': 'Simulation type',
            'options': {}
        }
        plots_dict['nevents_by_simtype_by_processingtype'] = {}
        plots_groups['main_preset'].extend(['nevents_by_simtype'])
    elif productiontype == 'DPD':
        plots_data['group_by']['nevents']['outputdatatype'] = {}
        plots_data['group_by']['aslots']['outputdatatype'] = {}
        plots_dict['nevents_by_outputdatatype'] = {
            'data': [],
            'title': 'Output data type',
            'options': {}
        }
        plots_dict['aslots_by_outputdatatype'] = {
            'data': [],
            'title': 'Output data type',
            'options': {}
        }
        plots_groups['main_preset'].extend(['aslots_by_outputdatatype'])
        plots_groups['extra'].extend(['nevents_by_outputdatatype'])


    # collect data for plots
    for task in task_list:
        for plot_type, pdict in plots_data.items():
            if plot_type == 'sum':
                for sumparam, byparams in pdict.items():
                    for byparam, keys in byparams.items():
                        if byparam == 'status':
                            for key in keys:
                                plots_data[plot_type][sumparam][byparam][key] += task[sumparam+key] if sumparam+key in task else 0
            elif plot_type == 'group_by':
                for sumparam, byparams in pdict.items():
                    for byparam in byparams:
                        if task[byparam] not in plots_data[plot_type][sumparam][byparam]:
                            plots_data[plot_type][sumparam][byparam][task[byparam]] = 0
                        plots_data[plot_type][sumparam][byparam][task[byparam]] += task[sumparam]
            elif plot_type == 'group_by_by':
                for sumparam, byparams in pdict.items():
                    for param in byparams:
                        byby_params = param.split('_')
                        if task[byby_params[0]] not in plots_data[plot_type][sumparam][param]:
                            plots_data[plot_type][sumparam][param][task[byby_params[0]]] = {}
                        if task[byby_params[1]] not in plots_data[plot_type][sumparam][param][task[byby_params[0]]]:
                            plots_data[plot_type][sumparam][param][task[byby_params[0]]][task[byby_params[1]]] = 0
                        plots_data[plot_type][sumparam][param][task[byby_params[0]]][task[byby_params[1]]] += task[sumparam]
            elif plot_type == 'hist':
                for param in pdict.keys():
                    plots_data[plot_type][param]['rawdata'].append(task[param])

    # build histograms
    N_BIN_MAX = 100
    for pname, pdata in plots_data['hist'].items():
        rawdata = pdata['rawdata']
        if len(rawdata) > 0:
            plots_data['hist'][pname]['stats'] = [np.average(rawdata), np.std(rawdata)]
            try:
                bins, ranges = np.histogram(rawdata, bins='auto')
            except MemoryError:
                bins, ranges = np.histogram(rawdata, bins=N_BIN_MAX)
            if len(ranges) > N_BIN_MAX + 1:
                bins, ranges = np.histogram(rawdata, bins=N_BIN_MAX)

            plots_data['hist'][pname]['data'] = [['x'], ['N tasks']]
            plots_data['hist'][pname]['data'][0].extend(list(np.ceil(ranges)))
            plots_data['hist'][pname]['data'][1].extend(list(np.histogram(rawdata, ranges)[0]))
        else:
            try:
                del (plots_data['hist'][pname])
            except:
                pass

    # inject plots data to plot dict
    to_delete = []
    extra_plots = {}
    for pname, pdict in plots_dict.items():
        if pname.count('_by') == 2:
            [sumparam, byparam, bybyparam] = pname.split('_by_')
            if sumparam in plots_data['group_by_by'] and byparam+'_'+bybyparam in plots_data['group_by_by'][sumparam]:
                for key, kdict in plots_data['group_by_by'][sumparam][byparam+'_'+bybyparam].items():
                    extra_plots[sumparam+'_'+key+'_by_'+bybyparam] = {
                        'data': [[k, v] for k, v in plots_data['group_by_by'][sumparam][byparam+'_'+bybyparam][key].items()],
                        'title': key,
                        'options': {
                            'total': sum(plots_data['group_by_by'][sumparam][byparam+'_'+bybyparam][key].values())
                        },
                    }
                    plots_groups['extra'].append(sumparam+'_'+key+'_by_'+bybyparam)
            to_delete.append(pname)
        elif pname.count('_by') == 1:
            [sumparam, byparam] = pname.split('_by_')
            if sumparam in plots_data['group_by'] and byparam in plots_data['group_by'][sumparam]:
                plots_dict[pname]['data'] = [[k, v] for k, v in plots_data['group_by'][sumparam][byparam].items() if v > 0]
                plots_dict[pname]['options']['total'] = sum(plots_data['group_by'][sumparam][byparam].values())
        elif '_hist' in pname:
            param = pname.split('_')[0]
            if param in plots_data['hist']:
                plots_dict[pname]['data'] = plots_data['hist'][param]['data']
                plots_dict[pname]['options']['stats'] = plots_data['hist'][param]['stats']
        elif '_sum_' in pname:
            [sumparam, byparam] = pname.split('_sum_')
            if sumparam in plots_data['sum'] and byparam in plots_data['sum'][sumparam]:
                plots_dict[pname]['data'] = [[k, v] for k, v in plots_data['sum'][sumparam][byparam].items() if v > 0]
                plots_dict[pname]['options']['total'] = sum(plots_data['sum'][sumparam][byparam].values())

        # check if plot is in one of main groups
        if pname not in plots_groups['main'] and pname not in plots_groups['main_preset'] and pname not in to_delete:
            plots_groups['extra'].append(pname)

    plots_dict.update(extra_plots)
    for key in to_delete:
        try:
            del plots_dict[key]
        except KeyError:
            pass

    # divide  plots by groups
    plots = {}
    for plots_group, pnames in plots_groups.items():
        plots[plots_group] = {}
        for pname in pnames:
            if pname in plots_dict:
                plots[plots_group][pname] = plots_dict[pname]

    return plots


def prepareNeventsByProcessingType(task_list):
    """
    Prepare data to save
    :param task_list:
    :return:
    """

    event_states = ['total', 'used', 'running', 'waiting']
    neventsByProcessingType = {}
    for task in task_list:
        if task['processingtype'] not in neventsByProcessingType:
            neventsByProcessingType[task['processingtype']] = {}
            for ev in event_states:
                neventsByProcessingType[task['processingtype']][ev] = 0
        neventsByProcessingType[task['processingtype']]['total'] += task['nevents'] if 'nevents' in task and task['nevents'] is not None else 0
        neventsByProcessingType[task['processingtype']]['used'] += task['neventsused'] if 'neventsused' in task and task['neventsused'] is not None else 0
        neventsByProcessingType[task['processingtype']]['running'] += task['neventsrunning'] if 'neventsrunning' in task and task['neventsrunning'] is not None else 0
        neventsByProcessingType[task['processingtype']]['waiting'] += (task['neventstobeused'] - task['neventsrunning']) if 'neventstobeused' in task and 'neventsrunning' in task and task['neventstobeused'] is not None and task['neventsrunning'] is not None else 0

    return neventsByProcessingType


def saveNeventsByProcessingType(neventsByProcessingType, qtime):
    """
    Save a snapshot of production state expressed in nevents in different states for various processingtype
    :param neventsByProcessingType:
    :param qtime:
    :return: True in case of successful save but False in case of an exception error
    """

    try:
        with transaction.atomic():
            for pt, data in neventsByProcessingType.items():
                row = ProdNeventsHistory(processingtype=pt,
                                         neventstotal=data['total'],
                                         neventsused=data['used'],
                                         neventswaiting=data['waiting'],
                                         neventsrunning=data['running'],
                                         timestamp=qtime)
                row.save()
    except DatabaseError as e:
        print (e.message)
        return False
    return True