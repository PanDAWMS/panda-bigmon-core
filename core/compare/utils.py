"""
    Created on 18.07.2018
    :author Tatiana Korchuganova
    Functions for comparison of PanDA object parameters
"""
import json
import urllib3
from core.compare.modelsCompare import ObjectsComparison

def add_to_comparison(objecttype, userid, value):

    oldComparison = None
    query = {}
    query['userid'] = userid
    query['object'] = objecttype
    try:
        oldComparison = ObjectsComparison.objects.get(**query)
    except ObjectsComparison.DoesNotExist:
        oldComparison = None
    if oldComparison:
        oldList = json.loads(oldComparison.comparisonlist)
        if oldList is None:
            oldList = []
        newList = oldList
        if value not in newList:
            newList.append(value)
        oldComparison.comparisonlist = json.dumps(newList)
        oldComparison.save()
    else:
        newList = [value]
        ObjectsComparison.objects.create(userid=userid, object=objecttype, comparisonlist=json.dumps(newList))


    return newList


def delete_from_comparison(objecttype, userid, value):
    oldComparison = None
    query = {}
    query['userid'] = userid
    query['object'] = objecttype
    try:
        oldComparison = ObjectsComparison.objects.get(**query)
    except ObjectsComparison.DoesNotExist:
        oldComparison = None
    if oldComparison:
        oldList = json.loads(oldComparison.comparisonlist)
        if oldList is not None:
            newList = oldList
            if value in newList:
                newList.remove(value)
        else:
             newList = []
        oldComparison.comparisonlist = json.dumps(newList)
        oldComparison.save()
    else:
        newList = []
        ObjectsComparison.objects.create(userid=userid, object=objecttype, comparisonlist=json.dumps(newList))
    return newList


def clear_comparison_list(objecttype, userid):
    isDeleted = False
    query = {}
    query['userid'] = userid
    query['object'] = objecttype

    try:
        oldComparisonList = ObjectsComparison.objects.get(**query)
    except ObjectsComparison.DoesNotExist:
        oldComparisonList = None
        isDeleted = True

    if oldComparisonList:
        oldComparisonList.comparisonlist = json.dumps([])
        oldComparisonList.save()
        isDeleted = True


    return isDeleted


def job_info_getter(url_params_str):
    """
    A function for getting jobs info in multithreading mode
    :return: dictionary with job info
    """
    base_url = "http://bigpanda.cern.ch/job"
    jobInfoDict = {}

    http = urllib3.PoolManager()
    resp = http.request('GET', base_url + url_params_str)
    jobInfoDict = json.loads(resp.data)

    return jobInfoDict