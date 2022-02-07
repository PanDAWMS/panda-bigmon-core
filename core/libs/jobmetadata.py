"""
Set of functions that query and process data from PanDA METADATA tables
Created by Tatiana Korchuganova
"""
import json
import logging
from datetime import datetime
from core.libs.exlib import get_tmp_table_name, insert_to_temp_table
from core.common.models import Metatable, MetatableArch

_logger = logging.getLogger('bigpandamon')


def addJobMetadata(jobs):
    """
    This function created backend dependable for avoiding numerous arguments in metadata query.
    Transaction and cursors used due to possible issues with django connection pooling
    :param jobs: list of dicts containing pandaid
    :return: jobs: expanded by 'metastruct' key
    """
    _logger.info('adding metadata')
    N_MAX_ITEMS_IN = 100
    useMetaArch = False
    pids = []
    for job in jobs:
        pids.append(job['pandaid'])
        if 'creationtime' in job:
            tdelta = datetime.now() - job['creationtime']
            delta = int(tdelta.days) + 1
            if delta > 3:
                useMetaArch = True

    mrecs = []
    # Get job metadata
    if len(jobs) < N_MAX_ITEMS_IN:
        # use IN where clause
        query = {}
        query['pandaid__in'] = pids
        mrecs.extend(Metatable.objects.filter(**query).values())
        if useMetaArch:
            mrecs.extend(MetatableArch.objects.filter(**query).values())
    else:
        # use tmp table
        tmpTableName = get_tmp_table_name()
        transactionKey = insert_to_temp_table(pids)
        query_str = ' pandaid IN (SELECT id FROM {} WHERE transactionkey={} ) '.format(tmpTableName, transactionKey)

        mrecs.extend(Metatable.objects.extra(where=[query_str]).values())
        if useMetaArch:
            mrecs.extend(MetatableArch.objects.extra(where=[query_str]).values())

    # add metadata to list of jobs
    mdict = {}
    if mrecs:
        for m in mrecs:
            try:
                mdict[m['pandaid']] = m['metadata']
            except:
                pass
    for job in jobs:
        if job['pandaid'] in mdict:
            try:
                job['metastruct'] = json.loads(mdict[job['pandaid']])
            except Exception as ex:
                _logger.exception('Failed to extract metadata for pandaid: {} with \n{}'.format(job['pandaid'], ex))
    _logger.info('added metadata')

    return jobs
