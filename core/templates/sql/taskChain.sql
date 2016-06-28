with tasks as (
	SELECT ListAgg(parent_tid,';')
	       within group(order by Level desc) as revPath
	FROM t_production_task
	START WITH taskid = %i
	CONNECT BY NOCYCLE PRIOR parent_tid = taskid
	),
    current_task as (
		select CASE WHEN ( INSTR(revPath,';') > 0 ) THEN substr(revPath,0,instr(revPath,';',1,1) - 1)
               ELSE revPath END as taskid
		from tasks
	),
	tasks_chain as (
		select ROWNUM as rnum,
			   t.taskid,
			   t.parent_tid,
			   t.taskname,
			   t.inputdataset,
			   TO_CHAR(t.timestamp, 'DD-MM-RR HH24:MI:SS.FF') as timestamp,
			   t.phys_group,
			   t.status,
			   t.username,
			   LEVEL as lvl
		from t_production_task t, current_task ct
		START WITH t.taskid = ct.taskid
		CONNECT BY NOCYCLE t.parent_tid = PRIOR t.taskid
		),
	datasets as (
		select t.rnum as rnum,
			   t.taskid as taskid,
		   	   t.parent_tid as parent_tid,
		   	   t.taskname as taskname,
		       t.lvl as lvl,
		       t.phys_group as phys_group,
		       t.status as status,
		       t.username as username,
		       t.timestamp as timestamp,
	LISTAGG(' {"name" : "' || d.name || '", "timestamp" : "' || d.timestamp || '", "status" : "' || d.status || '", "files" : "' || d.files || '", "events" : "' || d.events || '"}', ',')
	WITHIN GROUP (ORDER BY d.name,
						   d.timestamp,
						   d.status,
						   d.files,
						   d.events) as datasets
	from tasks_chain t, t_production_dataset d
	where  t.taskid = d.taskid
	GROUP BY t.rnum,
			 t.taskid,
			 t.parent_tid,
			 t.taskname,
			 t.lvl,
			 t.phys_group,
			 t.status,
			 t.username,
			 t.timestamp
	)
	select
	  CASE
	    /* the top dog gets a left curly brace to start things off */
	    WHEN lvl = 1 THEN '{'
	    /* when the last lvl is lower (shallower) than the current lvl, start a "children" array */
	    WHEN lvl - LAG(lvl) OVER (order by rnum) = 1 THEN ',"children" : [{'
	    ELSE ',{'
	  END
	  || ' "name" : "' || taskid || '", '
	  || ' "taskname" : "' || taskname || '", '
	  || ' "phys_group" : "' || phys_group || '", '
	  || ' "status" : "' || status || '", '
	  || ' "username" : "' || username || '", '
	  || ' "timestamp" : "' || timestamp || '", '
	  || ' "datasets" : [' || datasets || ']'
	  /* when the next lvl lower (shallower) than the current lvl, close a "children" array */
	  || CASE WHEN LEAD(lvl, 1, 1) OVER (order by rnum) - lvl <= 0
	     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(lvl, 1, 1) OVER (order by rnum) - lvl)), ']}' )
	     ELSE NULL
	  END as JSON_SNIPPET
	from datasets
	order by rnum
