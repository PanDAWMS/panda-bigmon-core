"""
All functions related to the data from ATLAS DEFT
"""
import logging
from django.db import connections
from core.libs.exlib import get_tmp_table_name, insert_to_temp_table, dictfetchall
from core.common.models import TRequest

from django.conf import settings

_logger = logging.getLogger('bigpandamon')

def extend_view_deft(request, query, extra_str):
    """
    Adding options to query and extra str in case of DEFT related parameters in request
    :return: query: dict
    :return: extra_str: extra query str
    """
    if 'hashtag' in request.session['requestParams']:
        hashtagsrt = request.session['requestParams']['hashtag']
        if ',' in hashtagsrt:
            hashtaglistquery = ''.join("'" + ht + "' ," for ht in hashtagsrt.split(','))
        elif '|' in hashtagsrt:
            hashtaglistquery = ''.join("'" + ht + "' ," for ht in hashtagsrt.split('|'))
        else:
            hashtaglistquery = "'" + request.session['requestParams']['hashtag'] + "'"
        hashtaglistquery = hashtaglistquery[:-1] if hashtaglistquery[-1] == ',' else hashtaglistquery
        extra_str += """ 
        and jeditaskid in ( 
            select htt.taskid 
            from atlas_deft.t_hashtag h, atlas_deft.t_ht_to_task htt 
            where jeditaskid = htt.taskid and h.ht_id = htt.ht_id and h.hashtag in ({})
            ) 
        """.format(hashtaglistquery)

    if 'tape' in request.session['requestParams']:
        where_tail_str = ''
        if 'stagesource' in request.session['requestParams']:
            where_tail_str = "and t1.source_rse='{}'".format(
                request.session['requestParams']['stagesource'].strip().replace("'", "''"))
        extra_str += """ 
        and jeditaskid in (
            select t2.taskid
            from atlas_deft.t_dataset_staging t1, atlas_deft.t_action_staging t2
            where t1.dataset_staging_id=t2.dataset_staging_id  {}
        )
        """.format(where_tail_str)
    return query, extra_str


def get_prod_slice_by_taskid(jeditaskid):
    """
    Get slice for a task from DEFT DB
    :param jeditaskid: int
    :return: slice
    """
    try:
        jsquery = """
            select tasks.taskid, tasks.pr_id, tasks.step_id, datasets.slice from atlas_deft.t_production_task tasks 
            join atlas_deft.t_production_step steps on tasks.step_id = steps.step_id 
            join atlas_deft.t_input_dataset datasets on datasets.ind_id=steps.ind_id  
            where tasks.taskid=:taskid
        """
        cur = connections['default'].cursor()
        cur.execute(jsquery, {'taskid': jeditaskid})
        task_prod_info = cur.fetchall()
        cur.close()
    except Exception as ex:
        task_prod_info = None
        _logger.exception('Failed to get slice by taskid from DEFT DB:\n{}'.format(ex))
    slice = None
    if task_prod_info:
        slice = task_prod_info[0][3]
    return slice


def get_prod_request_info(reqid_list, params=None):
    """
    Getting request info from DEFT DB
    :param reqid_list: list - list of production request id
    :param params: list - list of fields to get from DEFT DB
    :return: reqs: list - list of dicts
    """
    if len(reqid_list) == 0:
        return None
    pquery = {'reqid__in': reqid_list}
    if params is None:
        # get all fields
        values = [f.name for f in TRequest._meta.get_fields()]
    elif isinstance(params, list) and len(params) > 0:
        values = tuple(set(params) & set([f.name for f in TRequest._meta.get_fields()]))
    else:
        return None

    reqs = []
    reqs.extend(TRequest.objects.filter(**pquery).values(*values))

    return reqs



def staging_info_for_tasklist(request, tasks, transaction_key=None):
    """
    Get input dataset staging info for list of tasks and add it to it
    :param request:
    :param tasks: list of dicts
    :param transaction_key:
    :return: tasks: list of dicts, enriched with staging data
    :return: datasetstage
    """

    query_str = ''
    if 'stagesource' in request.session['requestParams']:
        if request.session['requestParams']['stagesource'] == 'Unknown':
            query_str = ' and t1.source_rse is null '
        else:
            query_str = " and t1.source_rse='{}' ".format(
                request.session['requestParams']['stagesource'].strip().replace("'", "''"))

    tmp_table_name = get_tmp_table_name()
    if transaction_key is None:
        # insert taskids into tmp table
        taskl = [t['jeditaskid'] for t in tasks]
        transaction_key = insert_to_temp_table(taskl)

    new_cur = connections["default"].cursor()
    new_cur.execute(
        """
        select t1.dataset, t1.status, t1.staged_files, t1.start_time, t1.end_time, t1.rse, t1.total_files, 
            t1.update_time, t1.source_rse, t2.taskid 
        from atlas_deft.t_dataset_staging t1
        inner join atlas_deft.t_action_staging t2 
        on t1.dataset_staging_id=t2.dataset_staging_id {} 
            and t2.taskid in (select tmp.id from {} tmp where tmp.transactionkey={})
        """.format(query_str, tmp_table_name, transaction_key)
    )
    datasetstage = dictfetchall(new_cur)
    taskslistfiltered = set()
    for datasetstageitem in datasetstage:
        taskslistfiltered.add(datasetstageitem['TASKID'])
        if datasetstageitem['START_TIME']:
            datasetstageitem['START_TIME'] = datasetstageitem['START_TIME'].strftime(settings.DATETIME_FORMAT)
        else:
            datasetstageitem['START_TIME'] = ''

        if datasetstageitem['END_TIME']:
            datasetstageitem['END_TIME'] = datasetstageitem['END_TIME'].strftime(settings.DATETIME_FORMAT)
        else:
            datasetstageitem['END_TIME'] = ''

        if not datasetstageitem['SOURCE_RSE']:
            datasetstageitem['SOURCE_RSE'] = 'Unknown'

        if datasetstageitem['UPDATE_TIME']:
            datasetstageitem['UPDATE_TIME'] = datasetstageitem['UPDATE_TIME'].strftime(settings.DATETIME_FORMAT)
        else:
            datasetstageitem['UPDATE_TIME'] = ''

    datasetRSEsHash = {}
    if len(datasetstage) > 0:
        for dataset in datasetstage:
            datasetRSEsHash[dataset['TASKID']] = dataset['SOURCE_RSE']

    # filter tasks by stagesource
    if 'stagesource' in request.session['requestParams']:
        tasks = [t for t in tasks if t['jeditaskid'] in taskslistfiltered]

    # add stagesource to task attributes
    for task in tasks:
        if task['jeditaskid'] in taskslistfiltered:
            task['stagesource'] = datasetRSEsHash.get(task['jeditaskid'], 'Unknown')

    return tasks, datasetstage


def hashtags_for_tasklist(request, tasks, transaction_key):
    """
    Getting list of hashtags for tasks from ATLAS DEFT table
    :param transaction_key: int: for list of tasks put into temporary table
    :return: tasks: list of dicts, enriched with hashtags
    :return: hashtags: list
    """
    hashtags = []
    tmp_table_name = get_tmp_table_name()
    if transaction_key is None:
        # insert taskids into tmp table
        taskl = [t['jeditaskid'] for t in tasks]
        transaction_key = insert_to_temp_table(taskl)
    new_cur = connections['default'].cursor()
    new_cur.execute(
        """
        select htt.taskid,
            listagg(h.hashtag, ',') within group (order by htt.taskid) as hashtags
        from atlas_deft.t_hashtag h, atlas_deft.t_ht_to_task htt, {} tmp
        where transactionkey={} and h.ht_id = htt.ht_id and tmp.id = htt.taskid
        group by htt.taskid
        """.format(tmp_table_name, transaction_key)
    )
    data_raw = dictfetchall(new_cur)

    task_hashtags = {}
    for row in data_raw:
        task_hashtags[row['TASKID']] = row['HASHTAGS']

    # Filtering tasks if there are a few hashtags with 'AND' operand in query
    if 'hashtag' in request.session['requestParams']:
        hashtag_list = request.session['requestParams']['hashtag'].split(',')
        tasks = [t for t in tasks if t['jeditaskid'] in task_hashtags and all(
            ht + ',' in task_hashtags[t['jeditaskid']] + ',' for ht in hashtag_list)
        ]

    for task in tasks:
        if task['jeditaskid'] in task_hashtags:
            task['hashtag'] = task_hashtags[task['jeditaskid']]
            # Forming hashtag list for summary attribute table
            for hashtag in task_hashtags[task['jeditaskid']].split(','):
                if hashtag not in hashtags:
                    hashtags.append(hashtag)

    if len(hashtags) > 0:
        hashtags = sorted(hashtags, key=lambda h: h.lower())

    return tasks, hashtags


def get_task_chain(jeditaskid):
    """
    Get task chain data for Gantt diagram.
    Author: Maria Grigorieva
    :param jeditaskid: int
    :return: task_chain_data
    """

    task_chain_data = []
    query = """
    with tasks as (
        SELECT ListAgg(parent_tid,';') within group(order by Level desc) as revPath from {DB_SCHEMA_DEFT}.t_production_task
        START WITH taskid = {tid} CONNECT BY NOCYCLE PRIOR parent_tid = taskid
    ),
    current_task as (
        select CASE WHEN ( INSTR(revPath,';') > 0 ) THEN substr(revPath,0,instr(revPath,';',1,1) - 1)
               ELSE revPath END as taskid from tasks
	),
	tasks_chain_ms as (
		select ROWNUM as rnum,
            t.taskid as id,
            t.taskname as taskname,
            substr(t.inputdataset, instrc(t.inputdataset,'.',1,4)+1,instrc(t.inputdataset,'.',1,5)-instrc(t.inputdataset,'.',1,4)-1) as input,
            (
                select ListAgg(substr(name, instrc(name,'.',1,4)+1,instrc(name,'.',1,5)-instrc(name,'.',1,4)-1),',') within group(order by name)
                from {DB_SCHEMA_DEFT}.t_production_dataset 
                where taskid = t.taskid and substr(name, instrc(name,'.',1,4)+1,instrc(name,'.',1,5)-instrc(name,'.',1,4)-1) != 'log'
            ) as output,
            (
                select ListAgg(name||':'||status,',') within group(order by status)
                from {DB_SCHEMA_DEFT}.t_production_dataset 
                where taskid = t.taskid and substr(name, instrc(name,'.',1,4)+1,instrc(name,'.',1,5)-instrc(name,'.',1,4)-1) != 'log') as aod_ds,
            substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as prod_step,
            CASE WHEN t.parent_tid = t.taskid THEN NULL ELSE t.parent_tid END as parent,
            NVL(TO_CHAR(t.start_time, 'yyyy-mm-dd hh24:mi:ss'),'NA') as start_time,
            NVL(TO_CHAR(cast(t.endtime as timestamp), 'yyyy-mm-dd hh24:mi:ss'),'NA') as end_time,
            NVL(TO_CHAR(t.ttcr_timestamp, 'yyyy-mm-dd hh24:mi:ss'),'NA') as ttcr_timestamp,
            NVL(TO_CHAR(current_timestamp, 'yyyy-mm-dd hh24:mi:ss'),'NA') as curr_time,
            t.total_req_jobs as total_req_jobs,
            t.total_done_jobs as total_done_jobs,
            (to_date(to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') - to_date('1970-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss'))*1000*60*60*24 as curr_time_millis,
            (to_date(to_char(t.start_time, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') - to_date('1970-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss'))*1000*60*60*24 as start_time_millis,
            (to_date(to_char(t.ttcr_timestamp, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') - to_date('1970-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss'))*1000*60*60*24 as ttcr_millis,
            t.status,
            LEVEL as lvl,
            substr(PRIOR t.taskname, instrc(PRIOR t.taskname,'.',1,3)+1,instrc(PRIOR t.taskname,'.',1,4)-instrc(PRIOR t.taskname,'.',1,3)-1) as parent_step,
            TO_CHAR(PRIOR t.start_time + INTERVAL '1' HOUR, 'yyyy-mm-dd hh24:mi:ss') as parent_start_time
		from {DB_SCHEMA_DEFT}.t_production_task t, current_task ct
		START WITH t.taskid = ct.taskid
		CONNECT BY NOCYCLE t.parent_tid = PRIOR t.taskid
		order siblings by t.taskid
	),
	task_chain_predicted_ms as (
    	select id as pID, parent as pParent, aod_ds as pAODds, taskname as pName, input as pInput, output as pOutput, '' as pLink, 0 as pMile, status as pStatus, 0 as pGroup, 1 as pOpen, '' as pCaption, '' as pNotes,
    		   CASE WHEN parent is not null and parent_step = 'evgen' THEN parent||'FS' ELSE parent||'SS' END as pDepend,
		       CASE WHEN status IN ('running','finished','done','submitting','submitted') THEN 'gtaskgreen'
		       	    WHEN status IN ('ready','paused','pending','waiting','toretry') THEN 'gtaskyellow'
		       	    WHEN status IN ('registered') THEN 'gtaskblue'
		       	    WHEN status IN ('obsolete') THEN 'gtaskgray'
		       	    ELSE 'gtaskred'
		       END as pClass,
    		   CASE WHEN input LIKE 'HIST%' and input = output THEN 'supermerge' WHEN input LIKE 'AOD%' and output LIKE 'DAOD%' THEN 'derive' ELSE prod_step END as pRes,
    		   CASE WHEN status IN ('registered') THEN parent_start_time ELSE start_time END as pStart,
    		   CASE WHEN status IN ('registered') THEN parent_start_time
    		        WHEN status IN ('submitting','submitted') THEN ttcr_timestamp
    		        ELSE
    		        (CASE WHEN end_time = 'NA' THEN to_char(TO_DATE('1970-01-01 00:00:00','yyyy-mm-dd hh24:mi.ss') + (
    		        	CASE WHEN status IN ('submitting','submitted','ready') THEN ttcr_millis
    		        	ELSE (
	    		        	CASE WHEN end_time = 'NA' and total_req_jobs > 0 and total_done_jobs > 0 THEN total_req_jobs / ( total_done_jobs / (curr_time_millis - start_time_millis) ) + start_time_millis
		    		   		WHEN end_time = 'NA' and total_req_jobs > 0 and total_done_jobs = 0 THEN total_req_jobs / ( (total_done_jobs+1) / (curr_time_millis - start_time_millis) ) + start_time_millis
		    		   		WHEN end_time = 'NA' and total_req_jobs = 0 THEN ttcr_millis ELSE ttcr_millis END
    		        	)
    		   			END
    		        	)/1000/60/60/24, 'yyyy-mm-dd hh24:mi:ss') ELSE end_time END)
    		   END as pEnd,
    		   CASE WHEN status IN ('finished','done') THEN 100
		       		WHEN status IN ('ready','pending','running',
		       						'toretry','paused','failed','aborted','broken','submitting','toabort','submitted','obsolete')
		       		THEN
		       			CASE WHEN total_req_jobs > 0 THEN
		       				(CASE WHEN round(total_done_jobs*100/total_req_jobs) > 100 THEN 100
		       				 ELSE round(total_done_jobs*100/total_req_jobs) END )
		       			ELSE 0 END
		       		ELSE 0
		       END as pComp
    	from tasks_chain_ms
    )
	SELECT xmltype.getclobval(xmlroot(xmlelement("project", XMLAGG(
	    XMLElement("task",
        XMLElement("pID", pID),
        XMLElement("pAODds", pAODds),
        XMLElement("pName", pName),
        XMLElement("pStart", pStart),
        XMLElement("pEnd", pEnd),
        XMLElement("pClass", pClass),
        XMLElement("pLink", pLink),
        XMLElement("pMile", pMile),
        XMLElement("pRes", pRes),
        XMLElement("pComp", pComp),
        XMLElement("pGroup", pGroup),
        XMLElement("pParent", pParent),
        XMLElement("pOpen", pOpen),
        XMLElement("pDepend", pDepend),
        XMLElement("pCaption", pCaption),
        XMLElement("pNotes", pNotes),
        XMLElement("pStatus", pStatus),
        XMLElement("pInput", pInput),
        XMLElement("pOutput", pOutput)
    ))), VERSION '1.0', STANDALONE YES)) xmldata
	from task_chain_predicted_ms
	""".format(tid=jeditaskid, DB_SCHEMA_DEFT='atlas_deft')

    new_cur = connections["default"].cursor()
    new_cur.execute(query)
    results = new_cur.fetchall()
    results_list = ["".join(map(str, r)) for r in results]
    results_str = results_list[0].replace("\n", "")
    substr_end = results_str.index(">")
    task_chain_data = results_str[substr_end + 1:]

    return task_chain_data
