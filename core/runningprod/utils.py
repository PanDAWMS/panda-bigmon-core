"""
Created by Tatiana Korchuganova on 01.10.2019
Utils for runningProdTasks module
"""


from django.db import transaction, DatabaseError
from core.runningprod.models import ProdNeventsHistory


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