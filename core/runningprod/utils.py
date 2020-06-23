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
        task['neventswaiting'] = task['neventstobeused']
        task['neventsrunning'] = task['neventsrunning'] if 'neventsrunning' in task and task['neventsrunning'] is not None else 0
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
            task['inputdataset'] = task['runnumber']
        else:
            task['inputdataset'] = None

        if task['inputdataset'] and task['inputdataset'].startswith('00'):
            task['inputdataset'] = task['inputdataset'][2:]

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
    plots_dict = {
        'neventsByStatus': {
            'data': [],
            'title': '',
            'options': {}
        },
        'neventsByTaskStatus': {
            'data': [],
            'title': '',
            'options': {}
        },
        'neventsByTaskPriority': {
            'data': [],
            'title': '',
            'options': {}
        },
        'neventsByProcessingType': {
            'data': [],
            'title': '',
            'options': {}
        },

        'taskAge': {
            'data': [],
            'title': '',
            'options': {}
        },
    }

    plots_data = {
        'sum': {
            'neventswaiting': 0,
            'neventsrunning': 0,
            'neventsused': 0,
        },
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
                'rawdata': []
            }
        }
    }

    if productiontype == 'MC':
        plots_data['group_by']['nevents']['FS_processingtype'] = {}
        plots_data['group_by']['nevents']['AFII_processingtype'] = {}
    elif productiontype == 'DPD':
        plots_data['group_by']['nevents']['outputdatatype'] = {}
        plots_data['group_by']['aslots']['outputdatatype'] = {}

    for task in task_list:
        for plot_type, pdict in plots_data.items():
            if plot_type == 'sum':
                for param in pdict.keys():
                    plots_data[plot_type][param] += task[param] if param in task else 0
            elif plot_type == 'group_by':
                for sumparam, byparams in pdict.items():
                    for byparam in byparams.keys():
                        if task[byparam] not in plots_data[plot_type][sumparam][byparam]:
                            plots_data[plot_type][sumparam][byparam][task[byparam]] = 0
                        plots_data[plot_type][sumparam][byparam][task[byparam]] += task[sumparam]
            elif plot_type == 'hist':
                for param in pdict.keys():
                    plots_data[plot_type][param]['rawdata'].append(task[param])

    N_BIN_MAX = 100
    for pname, pdata in plots_data['hist'].items():
        rawdata = pdata['rawdata']
        if len(rawdata) > 0:
            plots_data['hist'][pname]['stats'] = []
            plots_data['hist'][pname]['stats'].append(np.average(rawdata))
            plots_data['hist'][pname]['stats'].append(np.std(rawdata))
            try:
                bins, ranges = np.histogram(rawdata, bins='auto')
            except MemoryError:
                bins, ranges = np.histogram(rawdata, bins=N_BIN_MAX)
            if len(ranges) > N_BIN_MAX + 1:
                bins, ranges = np.histogram(rawdata, bins=N_BIN_MAX)

            plots_data['hist'][pname]['data'] = [[pname], ['N tasks']]
            plots_data['hist'][pname]['data'][0].extend(list(np.ceil(ranges)))
            plots_data['hist'][pname]['data'][1].extend(list(np.histogram(rawdata, ranges)[0]))
        else:
            try:
                del (plots_data['hist'][pname])
            except:
                pass


            

    return plots_dict


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