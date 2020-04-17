"""
Created on 18.12.2018
:author Tatiana Korchuganova
User specific functions
"""
from django.db import connection
from urllib.parse import urlencode, unquote, urlparse, urlunparse, parse_qs
from core.settings.base import ATLAS_DEPLOYMENT, BP_MON_SCHEMA, PANDA_SCHEMA, PANDAARCH_SCHEMA


def get_relevant_links(userid, fields):

    links = {'task': [], 'job': [], 'other': []}
    sqlquerystr = """
        select pagegroup, pagename,visitrank, url
        from (
            select sum(w) as visitrank, pagegroup, pagename, row_number() over (partition by pagegroup ORDER BY sum(w) desc) as rn, url
            from (
              select exp(-(SYSdate - cast(time as date))*24/12) as w,
              SUBSTR(url,
                    INSTR(url,'/',1,3)+1,
                    (INSTR(url,'/',1,4)-INSTR(url,'/',1,3)-1)) as pagename,
              case when SUBSTR(url,INSTR(url,'/',1,3)+1,(INSTR(url,'/',1,4)-INSTR(url,'/',1,3)-1)) in ('task', 'tasks', 'runningprodtasks', 'tasknew') then 'task'
                 when SUBSTR(url,INSTR(url,'/',1,3)+1,(INSTR(url,'/',1,4)-INSTR(url,'/',1,3)-1)) in ('job', 'jobs', 'errors') then 'job'
                 else 'other' end as pagegroup,
              case when url like '%%timestamp=%%' then SUBSTR(url,1,INSTR(url,'timestamp=',1,1)-2)
                   when url like '%%job?pandaid=%%' then REPLACE(url,'job?pandaid=','job/')
                   when url like '%%task?jeditaskid=%%' then REPLACE(url,'task?jeditaskid=','task/') else url end as url
              from """+BP_MON_SCHEMA+""".visits
              where USERID=%i and url not like '%%/user/%%' and INSTR(url,'/',1,4) != 0
              order by time DESC)
            group by pagegroup,pagename,url)
        where rn < 10""" % (userid)
    cur = connection.cursor()
    cur.execute(sqlquerystr)
    visits = cur.fetchall()
    cur.close()

    visitsnames = ['pagegroup', 'pagename', 'visitrank', 'url']
    relevantVisits = [dict(zip(visitsnames, row)) for row in visits]

    # adding non-standard url params to fields dict
    fields['task'].extend(['preset', 'processingtype', 'workinggroup', 'campaign'])
    fields['other'] = ['instance', 'package', 'branch', 'ntag_full']

    for page in relevantVisits:
        for link in links.keys():
            if page['pagegroup'].startswith(link):
                links[link].append(page)

    for link in links['task']:
        link['keyparams' ] =[]
        link['otherparams'] = []
        if link['pagename'] == 'task':
            link['linktext'] = link['url'].split('/')[4]
            link['keyparams'].append(dict(param='jeditaskid', value=link['url'].split('/')[4],
                                          importance=True))
        if link['pagename'] in ['tasks', 'runningprodtasks']:
            link['linktext'] = link['pagename']
            params = unquote(urlparse(link['url']).query).split('&')
            for param in params:
                if '=' in param:
                    flag = False
                    if param.split('=')[0] in fields['task']:
                        flag = True
                        link['keyparams'].append(dict(param=param.split('=')[0], value=param.split('=')[1],
                                                      importance=flag))
                    else:
                        link['otherparams'].append(dict(param=param.split('=')[0], value=param.split('=')[1],
                                                        importance=flag))

    for link in links['job']:
        link['keyparams'] = []
        link['otherparams'] = []
        if link['pagename'] == 'job':
            link['linktext'] = link['url'].split('/')[4]
            link['keyparams'].append(dict(param='pandaid', value=link['url'].split('/')[4],
                                          importance=True))
        if link['pagename'] == 'jobs':
            link['linktext'] = link['pagename']
            params = unquote(urlparse(link['url']).query).split('&')
            for param in params:
                if '=' in param:
                    flag = False
                    if param.split('=')[0] in fields['job']:
                        flag = True
                        link['keyparams'].append(dict(param=param.split('=')[0], value=param.split('=')[1],
                                                      importance=flag))
                    else:
                        link['otherparams'].append(dict(param=param.split('=')[0], value=param.split('=')[1],
                                                        importance=flag))

    for link in links['other']:
        link['keyparams'] = []
        link['otherparams'] = []
        if link['pagename'] == 'site':
            link['linktext'] = link['pagename']
            link['keyparams'].append(dict(param='site', value=link['url'].split('/')[4],
                                          importance=True))
        elif link['pagename'] == 'dash':
            link['linktext'] = link['pagename']
            link['keyparams'].append(dict(param='tasktype', value=link['url'].split('/')[4],
                                          importance=True))
        else:
            link['linktext'] = link['pagename']
        params = unquote(urlparse(link['url']).query).split('&')
        for param in params:
            if '=' in param:
                flag = False
                if param.split('=')[0] in fields['site'] or param.split('=')[0] in fields['job'] or \
                        param.split('=')[0] in fields['task'] or param.split('=')[0] in fields['other']:
                    flag = True
                    link['keyparams'].append(dict(param=param.split('=')[0], value=param.split('=')[1],
                                                  importance=flag))
                else:
                    link['otherparams'].append(dict(param=param.split('=')[0], value=param.split('=')[1],
                                                    importance=flag))
    return links
