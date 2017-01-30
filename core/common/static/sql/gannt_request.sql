with tasks as (
	SELECT ListAgg(parent_tid,';') within group(order by Level desc) as revPath FROM t_production_task
	START WITH taskid = %i CONNECT BY NOCYCLE PRIOR parent_tid = taskid
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
			   (select ListAgg(substr(name, instrc(name,'.',1,4)+1,instrc(name,'.',1,5)-instrc(name,'.',1,4)-1),',') within group(order by name) 
	            from t_production_dataset where taskid = t.taskid and substr(name, instrc(name,'.',1,4)+1,instrc(name,'.',1,5)-instrc(name,'.',1,4)-1) != 'log') as output,
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
		from t_production_task t, current_task ct 
		START WITH t.taskid = ct.taskid 
		CONNECT BY NOCYCLE t.parent_tid = PRIOR t.taskid
		order siblings by t.taskid
		),
	task_chain_predicted_ms as (
    	select id as pID, parent as pParent, taskname as pName, input as pInput, output as pOutput, '' as pLink, 0 as pMile, status as pStatus, 0 as pGroup, 1 as pOpen, '' as pCaption, '' as pNotes,
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
	SELECT xmltype.getclobval(xmlroot(xmlelement("project",
		   XMLAGG(XMLELEMENT ("task",
		   	 	   XMLElement("pID", pID),
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
                   XMLElement("pOutput", pOutput)))), VERSION '1.0', STANDALONE YES)) xmldata
	from task_chain_predicted_ms
