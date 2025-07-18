"""
    A set of views for DataCarousel app
"""

import json
import math
import logging
import time

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.cache import never_cache
from django.utils import timezone

from core.libs.checks import is_positive_int_field
from core.libs.exlib import build_time_histogram, convert_bytes, convert_sec, round_to_n_digits
from core.libs.DateEncoder import DateEncoder
from core.libs.task import get_datasets_for_tasklist
from core.oauth.utils import login_customrequired
from core.views import initRequest, setupView
from core.datacarousel.utils import getBinnedData, get_staging_data, send_report_rse, substitudeRSEbreakdown, staging_rule_verification, \
    get_stuck_files_data, setup_view_dc
import core.datacarousel.constants as const

from django.conf import settings

_logger = logging.getLogger('bigpandamon')


@never_cache
@login_customrequired
def data_carousel_dash(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=24, limit=9999999, querytype='task', wildCardExt=True)

    if query and 'modificationtime__castdate__range' in query:
        request.session['timerange'] = query['modificationtime__castdate__range']

    request.session['viewParams']['selection'] = ''
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
        'requestParams': request.session['requestParams'] if 'requestParams' in request.session else {},
        'timerange': request.session['timerange'],
    }

    response = render(request, 'dataCarouselDash.html', data, content_type='text/html')
    return response



@never_cache
def get_staging_info_for_task(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    extra_query_str = setup_view_dc(request)
    data_raw = get_staging_data(extra_query_str, add_idds_data=True)

    # prepare data for template
    datasets = []
    if data_raw and len(data_raw) > 0:
        for dsdata in data_raw:
            data = {}
            data['start_time_ms'] = 0

            for key in ('taskid', 'status', 'scope', 'dataset', 'rse', 'source_rse', 'destination_rse',
                        'step_action_id', 'source_rse_old'):
                data[key] = dsdata[key] if key in dsdata else '---'
                if key == 'dataset':
                    val = dsdata[key]
                    data[key] = val.split(':', 1)[1] if ':' in val else val

            for key in ('start_time', 'end_time'):
                if key in dsdata and dsdata[key] and isinstance(dsdata[key], timezone.datetime):
                    data[key] = dsdata[key].strftime(settings.DATETIME_FORMAT)
                    if key == 'start_time':
                        data['start_time_ms'] = int(dsdata[key].timestamp() * 1000)
                else:
                    data[key] = '---'

            if 'update_time' in dsdata and dsdata['update_time'] is not None:
                data['update_time'] = convert_sec(dsdata['update_time'].total_seconds(), out_unit='str')
            else:
                data['update_time'] = '---'

            data['total_files'] = dsdata['total_files'] if is_positive_int_field(dsdata, 'total_files') else 0
            data['staged_files'] = dsdata['staged_files'] if is_positive_int_field(dsdata, 'staged_files') else 0

            if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                data['staged_files_pct'] = round_to_n_digits(
                    dsdata['staged_files'] * 100.0 / dsdata['total_files'], 1, method='floor')
            else:
                data['staged_files_pct'] = 0

            if is_positive_int_field(dsdata, 'dataset_bytes'):
                data['total_bytes'] = round_to_n_digits(convert_bytes(dsdata['dataset_bytes'], output_unit='GB'), 2)
            else:
                data['total_bytes'] = 0

            if is_positive_int_field(dsdata, 'staged_bytes'):
                data['staged_bytes'] = round_to_n_digits(convert_bytes(dsdata['staged_bytes'], output_unit='GB'), 2)
            else:
                data['staged_bytes'] = 0

            if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                data['staged_bytes_pct'] = round_to_n_digits(
                    dsdata['staged_bytes'] * 100.0 / dsdata['dataset_bytes'], 1, method='floor')
            else:
                data['staged_bytes_pct'] = 0

            data['idds_status'] = dsdata['idds_status'] if 'idds_status' in dsdata else '---'
            data['idds_request_id'] = dsdata['idds_request_id'] if 'idds_request_id' in dsdata else 0
            data['idds_out_processed_files'] = dsdata['idds_out_processed_files'] if 'idds_out_processed_files' in dsdata else 0
            data['idds_out_total_files'] = dsdata['idds_out_total_files'] if 'idds_out_total_files' in dsdata else 0
            data['idds_pctprocessed'] = dsdata['idds_pctprocessed'] if 'idds_pctprocessed' in dsdata else 0

            datasets.append(data)

    response = JsonResponse(datasets, safe=isinstance(datasets, dict), content_type='application/json')
    return response

def build_summary_data(staginData):

    summary = {
        'processingtype': {},
        'source_rse': {},
        'campaign': {},
        'username': {},
        'destination_rse': {},
    }
    timelistSubmitted = []
    timelistSubmittedFiles = []
    timelistIntervalfin = []
    timelistIntervalact = []
    timelistIntervalqueued = []
    progressDistribution = []
    seen_request_ids = set()

    calc_temp = {
        "ds_active": 0, "ds_done": 0, "ds_queued": 0, "ds_90pdone": 0,
        'files_total': 0, "files_rem": 0, "files_queued": 0, "files_done": 0, 'files_active': 0,
        'bytes_total': 0, 'bytes_done': 0, "bytes_queued": 0, 'bytes_rem': 0, 'bytes_active': 0,
    }

    for dsdata in staginData:
        if dsdata.get('request_id') in seen_request_ids:
            continue
        seen_request_ids.add(dsdata.get('request_id'))

        for key in summary:
            if key == 'destination_rse' and dsdata['status'] == 'queued':
                continue

            if dsdata[key] not in summary[key]:
                summary[key][dsdata[key]] = {key: dsdata[key]}
                summary[key][dsdata[key]].update(calc_temp)
                if key == "source_rse":
                    summary[key][dsdata[key]]['source_rse_breakdown'] = substitudeRSEbreakdown(dsdata['source_rse'])

            if is_positive_int_field(dsdata, 'total_files'):
                summary[key][dsdata[key]]['files_total'] += dsdata['total_files']
            if is_positive_int_field(dsdata, 'staged_files'):
                summary[key][dsdata[key]]['files_done'] += dsdata['staged_files']
            if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                summary[key][dsdata[key]]['files_rem'] += dsdata['total_files'] - dsdata['staged_files']
            if is_positive_int_field(dsdata, 'dataset_bytes'):
                summary[key][dsdata[key]]['bytes_total'] += convert_bytes(dsdata['dataset_bytes'], 'GB')
            if is_positive_int_field(dsdata, 'staged_bytes'):
                summary[key][dsdata[key]]['bytes_done'] += convert_bytes(dsdata['staged_bytes'], 'GB')
            if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                summary[key][dsdata[key]]['bytes_rem'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], 'GB')

            if dsdata['end_time'] is not None:
                summary[key][dsdata[key]]['ds_done'] += 1
                timelistIntervalfin.append(dsdata['end_time'] - dsdata['start_time'])
            elif dsdata['status'] != 'queued':
                summary[key][dsdata[key]]['ds_active'] += 1
                timelistIntervalact.append(timezone.now() - dsdata['start_time'])
                if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                    summary[key][dsdata[key]]['files_active'] += dsdata['total_files'] - dsdata['staged_files']
                if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                    summary[key][dsdata[key]]['bytes_active'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], 'GB')
                if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files') and dsdata['staged_files'] >= dsdata['total_files'] * 0.9:
                    summary[key][dsdata[key]]['ds_90pdone'] += 1
            elif dsdata['status'] == 'queued':
                summary[key][dsdata[key]]['ds_queued'] += 1
                timelistIntervalqueued.append(timezone.now() - dsdata['creation_time'])
                if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                    summary[key][dsdata[key]]['files_queued'] += dsdata['total_files'] - dsdata['staged_files']
                if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                    summary[key][dsdata[key]]['bytes_queued'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], 'GB')

        if dsdata['start_time'] is not None:
            timelistSubmitted.append(dsdata['start_time'])
            timelistSubmittedFiles.append([dsdata['start_time'], dsdata['total_files']])
        if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
            progressDistribution.append(dsdata['staged_files'] / dsdata['total_files'])

    return summary, timelistSubmitted, timelistSubmittedFiles, timelistIntervalact, timelistIntervalfin, timelistIntervalqueued, progressDistribution

def build_dataset_list(staginData):
    dataset_list = []

    for dsdata in staginData:
        epltime = None
        if dsdata.get('end_time') and dsdata.get('start_time'):
            epltime = dsdata['end_time'] - dsdata['start_time']
        elif dsdata.get('start_time'):
            epltime = timezone.now() - dsdata['start_time']
        elif dsdata.get('creation_time'):
            epltime = timezone.now() - dsdata['creation_time']

        dataset_list.append({
            'campaign': dsdata['campaign'],
            'username': dsdata['username'],
            'pr_id': dsdata['pr_id'],
            'taskid': dsdata['taskid'],
            'dataset': dsdata['dataset'],
            'status': dsdata['status'],
            'total_files': dsdata['total_files'] if is_positive_int_field(dsdata, 'total_files') else 0,
            'staged_files': dsdata['staged_files'] if is_positive_int_field(dsdata, 'staged_files') else 0,
            'size': int(round(convert_bytes(dsdata['dataset_bytes'], 'GB'))) if is_positive_int_field(dsdata, 'dataset_bytes') else "0",
            'progress': int(math.floor(dsdata['staged_files'] * 100.0 / dsdata['total_files'])) if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files') else 0,
            'source_rse': dsdata['source_rse'],
            'destination_rse': dsdata['destination_rse'] if dsdata.get('destination_rse') else '---',
            'elapsedtime': convert_sec(epltime.total_seconds(), 'str') if epltime is not None else '---',
            'start_time': dsdata['start_time'].strftime(settings.DATETIME_FORMAT) if dsdata['start_time'] else '---',
            'rrule': dsdata['rse'],
            'update_time': convert_sec(dsdata['update_time'].total_seconds(), 'str') if dsdata.get('update_time') else '---',
            'processingtype': dsdata['processingtype']
        })
    return dataset_list

@never_cache
def get_data_carousel_data(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    extra_query_str = setup_view_dc(request)
    staginData = get_staging_data(extra_query_str, add_idds_data=False)

    summary, time_subm, time_subm_files, t_act, t_fin, t_queued, progress = build_summary_data(staginData)
    dataset_list = build_dataset_list(staginData)

    selection_options = {}
    for key in summary:
        selection_options[key] = sorted(
            [{"name": value, "value": value, "selected": "0"} for value in summary[key]],
            key=lambda x: x['name'].lower()
        )

    for param in summary:
        for value in summary[param]:
            for key in summary[param][value]:
                if key.startswith('bytes'):
                    summary[param][value][key] = int(round(summary[param][value][key]))
        summary[param] = sorted(list(summary[param].values()), key=lambda x: x[param].lower())

    binned_subm_datasets = build_time_histogram(time_subm) if time_subm else {}
    binned_subm_files = build_time_histogram(time_subm_files) if time_subm_files else {}
    binnedActFinData = getBinnedData(t_act, t_fin, t_queued)
    eplTime = [['Time', 'Active staging', 'Finished staging', 'Queued staging']] + [
        [round(time_str, 1), data[0], data[1], data[2]] for (time_str, data) in binnedActFinData
    ]

    _logger.debug('Prepared data: {}'.format(time.time() - request.session['req_init_time']))

    finalvalue = {
        "elapsedtime": eplTime,
        "submittime": [['Time', 'Count']] + [[time_str, data[0]] for time_str, data in binned_subm_datasets],
        "submittimefiles": [['Time', 'Count']] + [[time_str, data[0]] for time_str, data in binned_subm_files],
        "progress": [["Progress"]] + [[x * 100] for x in progress],
        "summary": summary,
        "selection": selection_options,
        "detailstable": dataset_list
    }
    return HttpResponse(json.dumps(finalvalue, cls=DateEncoder), content_type='application/json')

def get_stuck_files(request):
    """
    Return list of probably stuck in staging files for a dataset or Rucio rule & Rucio Storage Element
    :param request:
    :return: stuck_files
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    datasetname = None
    rule_id = None
    source_rse = None
    if 'rule_id' in request.session['requestParams'] and request.session['requestParams']['rule_id']:
        rule_id = request.session['requestParams']['rule_id']
    if 'source_rse' in request.session['requestParams'] and request.session['requestParams']['source_rse']:
        source_rse = request.session['requestParams']['source_rse']

    stuck_files = {}
    if rule_id and source_rse:
        stuck_files = get_stuck_files_data(rule_id, source_rse)

    # dict -> list for table
    stuck_files_list = []
    for f, data in stuck_files.items():
        stuck_files_list.extend(data['transfers'])

    return JsonResponse({'data': stuck_files_list})


@never_cache
def send_stalled_requests_report(request):
    """
    Send report about stalled requests to Data Carousel experts
    :param request:
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    # it is ATLAS specific view -> return no content
    if 'ATLAS' not in settings.DEPLOYMENT:
        return JsonResponse({'sent': 0}, status=204)

    output_stats = {}

    # get data
    extra_query_str = setup_view_dc(request)
    extra_query_str += f"""
        and t1.end_time is null and t1.status = 'staging' and t3.status not in ('cancelled','failed','broken','aborted','finished','done') 
        and (t1.last_staged_time <= sysdate - {const.DATA_CAROUSEL_MAIL_DELAY_DAYS} 
            or (t1.last_staged_time is null and t1.start_time <= sysdate - {const.DATA_CAROUSEL_MAIL_DELAY_DAYS}))
    """
    rows = get_staging_data(extra_query_str, add_idds_data=False)
    rows = sorted(rows, key=lambda x: x['update_time'], reverse=True)

    # check if nfiles in files == staged_files to avoid lost files case
    jedi_datasets_dict = {}
    jedi_datasets = get_datasets_for_tasklist([{'jeditaskid': r['taskid']} for r in rows])
    for task in jedi_datasets:
        for ds in task['datasets']:
            if ds['type'] == 'input' and ds['datasetname'] not in jedi_datasets_dict:
                jedi_datasets_dict[ds['datasetname']] = {
                    'dataset': ds['datasetname'],
                    'nfiles': ds['nfiles'],
                    'taskid': task['jeditaskid'],
                }
    rows = [r for r in rows if r['dataset'] in jedi_datasets_dict and jedi_datasets_dict[r['dataset']]['nfiles'] >= r['staged_files']]

    ds_per_rse = {}
    for r in rows:
        if r['source_rse'] not in ds_per_rse:
            ds_per_rse[r['source_rse']] = {}
        if r['rse'] not in ds_per_rse[r['source_rse']]:
            ds_per_rse[r['source_rse']][r['rse']] = {
                "se": r['source_rse'],
                "rr": r['rse'],
                "dataset": r['dataset'] if ':' not in r['dataset'] else r['dataset'].split(':')[1],
                "start_time": r['start_time'].strftime(settings.DATETIME_FORMAT),
                "tot_files": r['total_files'],
                "staged_files": r['staged_files'],
                "update_time": str(r['update_time']).split('.')[0] if r['update_time'] is not None else '',
                "tasks": [],
                "is_tape_problem": False,
                "stuck_files": [],
            }
        if r['taskid'] not in ds_per_rse[r['source_rse']][r['rse']]['tasks']:
            ds_per_rse[r['source_rse']][r['rse']]['tasks'].append(r['taskid'])

    # check if a tape is a reason of stalled staging
    for source_rse, rucio_rules in ds_per_rse.items():
        for rule in rucio_rules:
            rucio_rules[rule]["is_tape_problem"], rucio_rules[rule]["stuck_files"] = staging_rule_verification(rule, source_rse)

    # dict -> list of rules & send
    for rse, rucio_rules in ds_per_rse.items():
        _logger.debug("DataCarouselMails processes this RSE: {}".format(rse))
        if rse not in output_stats:
            output_stats[rse] = {0:0, 1:0, 2:0}
        data_email_categories = {
            'experts_only': [rule for r_uid, rule in rucio_rules.items() if rule['is_tape_problem'] is False],
            'site_admins': [rule for r_uid, rule in rucio_rules.items() if rule['is_tape_problem'] is True]
        }

        if len(data_email_categories['experts_only']) > 0:
            result = send_report_rse(
                rse,
                {
                    'rse': rse,
                    'name': 'Rules stuck due to issues most probably not related to Tape',
                    'rules': data_email_categories['experts_only'],
                },
                experts_only=True
            )
            output_stats[rse][result] += 1
        if len(data_email_categories['site_admins']) > 0:
            result = send_report_rse(
                rse,
                {
                    'rse': rse,
                    'name': 'Rules stuck with failures suspicious to be related to staging from Tape',
                    'rules': data_email_categories['site_admins'],
                },
                experts_only=False
            )
            output_stats[rse][result] += 1

    return JsonResponse({'sent': output_stats})

