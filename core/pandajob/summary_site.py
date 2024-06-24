"""
A set of functions to get jobs from JOBS* and group them by a site and cloud
"""
import logging
import copy
import time
import itertools
import re

from django.db.models import Count
from django.core.cache import cache

from core.schedresource.utils import getCRICSites, get_basic_info_for_pqs, get_pq_clouds, get_panda_queues
from core.libs.exlib import getPilotCounts
from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4

import core.constants as const

_logger = logging.getLogger('bigpandamon')


def site_summary_dict(sites, vo_mode='atlas', sortby='alpha'):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    sumd['copytool'] = {}
    for site in sites:
        for f in const.SITE_FIELDS_STANDARD:
            if f in site and site[f] is not None:
                if f not in sumd:
                    sumd[f] = {}
                if site[f] not in sumd[f]:
                    sumd[f][site[f]] = 0
                sumd[f][site[f]] += 1
        if 'copytool' in const.SITE_FIELDS_STANDARD:
            if 'copytools' in site and site['copytools'] and len(site['copytools']) > 0:
                copytools = list(site['copytools'].keys())
                for cp in copytools:
                    if cp not in sumd['copytool']:
                        sumd['copytool'][cp] = 0
                    sumd['copytool'][cp] += 1

    if vo_mode != 'atlas':
        try:
            del sumd['cloud']
        except:
            _logger.exception('Failed to remove cloud key from dict')

    # convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
        # sorting
        if sortby == 'count':
            iteml = sorted(iteml, key=lambda x: -x['kvalue'])
        else:
            iteml = sorted(iteml, key=lambda x: x['kname'])
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml

