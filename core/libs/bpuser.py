"""
Created on 18.12.2018
:author Tatiana Korchuganova
User specific functions
"""
import json

from django.db import connection
from urllib.parse import unquote, urlparse

from core.oauth.models import BPUser, BPUserSettings

from django.conf import settings


def get_relevant_links(userid, fields):

    links = {'task': [], 'job': [], 'other': []}
    sqlquerystr = """
        select pagegroup, pagename,visitrank, url
        from (
            select sum(w) as visitrank, pagegroup, pagename, row_number() over (partition by pagegroup order by sum(w) desc) as rn, url
            from (
              select exp(-(sysdate - cast(time as date))*24/12) as w,
              substr(url,
                    instr(url,'/',1,3)+1,
                    (instr(url,'/',1,4)-instr(url,'/',1,3)-1)) as pagename,
              case when substr(url,instr(url,'/',1,3)+1,(instr(url,'/',1,4)-INSTR(url,'/',1,3)-1)) in ('task', 'tasks', 'runningprodtasks', 'tasknew') then 'task'
                 when substr(url,instr(url,'/',1,3)+1,(instr(url,'/',1,4)-instr(url,'/',1,3)-1)) in ('job', 'jobs', 'errors') then 'job'
                 else 'other' end as pagegroup,
              case when url like '%%timestamp=%%' then substr(url,1,instr(url,'timestamp=',1,1)-2)
                   when url like '%%job?pandaid=%%' then replace(url,'job?pandaid=','job/')
                   when url like '%%task?jeditaskid=%%' then replace(url,'task?jeditaskid=','task/') else url end as url
              from {}.visits
              where userid={} and url not like '%%/user/%%' and instr(url,'/',1,4) != 0
              order by time desc) foo1
            group by pagegroup,pagename,url) foo2
        where rn < 10""".format(settings.DB_SCHEMA, userid)
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


def filterErrorData(request, data, **kwargs):

    if 'standard_errorfields' in kwargs:
        standard_errorfields = kwargs['standard_errorfields']
    else:
        standard_errorfields = ['cloud', 'computingsite', 'eventservice', 'produsername', 'jeditaskid', 'jobstatus',
            'processingtype', 'prodsourcelabel', 'specialhandling', 'taskid', 'transformation',
            'workinggroup', 'reqid', 'computingelement']

    defaultErrorsPreferences = {
        'jobattr': standard_errorfields,
    }

    defaultErrorsPreferences['tables'] = {
        'jobattrsummary': 'Job attribute summary',
        'errorsummary': 'Overall error summary',
        'siteerrorsummary': 'Site error summary',
        'usererrorsummary': 'User error summary',
        'taskerrorsummary': 'Task error summary'
    }
    userids = BPUser.objects.filter(email=request.user.email).values('id')
    userid = userids[0]['id']
    try:
        userSetting = BPUserSettings.objects.get(page='errors', userid=userid)
        userPreferences = json.loads(userSetting.preferences)
    except:
        saveUserSettings(request, 'errors')
        userSetting = BPUserSettings.objects.get(page='errors', userid=userid)
        userPreferences = json.loads(userSetting.preferences)
        # userPreferences = defaultErrorsPreferences

    userPreferences['defaulttables'] = defaultErrorsPreferences['tables']
    userPreferences['defaultjobattr'] = defaultErrorsPreferences['jobattr']
    ###TODO Temporary fix. Need to redesign
    userPreferences['jobattr'].append('reqid')
    userPreferences['jobattr'].append('computingelement')

    data['userPreferences'] = userPreferences
    if 'tables' in userPreferences:
        if 'jobattrsummary' in userPreferences['tables']:
            if 'jobattr' in userPreferences:
                sumd_new = []
                for attr in userPreferences['jobattr']:
                    for field in data['sumd']:
                        if attr == field['field']:
                            sumd_new.append(field)
                            continue
                data['sumd'] = sorted(sumd_new, key=lambda x: x['field'])
        else:
            try:
                del data['sumd']
            except:
                pass
        if 'errorsummary' not in userPreferences['tables']:
            try:
                del data['errsByCount']
            except:
                pass
        if 'siteerrorsummary' not in userPreferences['tables']:
            try:
                del data['errsBySite']
            except:
                pass
        if 'usererrorsummary' not in userPreferences['tables']:
            try:
                del data['errsByUser']
            except:
                pass
        if 'taskerrorsummary' not in userPreferences['tables']:
            try:
                del data['errsByTask']
            except:
                pass

    return data


def saveUserSettings(request, page, **kwargs):

    if 'standard_errorfields' in kwargs:
        standard_errorfields = kwargs['standard_errorfields']
    else:
        standard_errorfields = ['cloud', 'computingsite', 'eventservice', 'produsername', 'jeditaskid', 'jobstatus',
            'processingtype', 'prodsourcelabel', 'specialhandling', 'taskid', 'transformation',
            'workinggroup', 'reqid', 'computingelement']

    if page == 'errors':
        errorspage_tables = ['jobattrsummary', 'errorsummary', 'siteerrorsummary', 'usererrorsummary',
                            'taskerrorsummary']
        preferences = {}
        if 'jobattr' in request.session['requestParams']:
            preferences["jobattr"] = request.session['requestParams']['jobattr'].split(",")
            try:
                del request.session['requestParams']['jobattr']
            except:
                pass
        else:
            preferences["jobattr"] = standard_errorfields
        if 'tables' in request.session['requestParams']:
            preferences['tables'] = request.session['requestParams']['tables'].split(",")
            try:
                del request.session['requestParams']['tables']
            except:
                pass
        else:
            preferences['tables'] = errorspage_tables
        query = {}
        query['page']= str(page)
        if request.user.is_authenticated:
            userids = BPUser.objects.filter(email=request.user.email).values('id')
            userid = userids[0]['id']
            try:
                userSetting = BPUserSettings.objects.get(page=page, userid=userid)
                userSetting.preferences = json.dumps(preferences)
                userSetting.save(update_fields=['preferences'])
            except BPUserSettings.DoesNotExist:
                userSetting = BPUserSettings(page=page, userid=userid, preferences=json.dumps(preferences))
                userSetting.save()



