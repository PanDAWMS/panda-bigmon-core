"""
Created by Tatiana Korchuganova on 04.10.2019
"""

from datetime import datetime, timedelta

from django.db.models.functions import Substr

from core.views import preprocessWildCardString
from core.art.modelsART import ARTTests



artdateformat = '%Y-%m-%d'


def setupView(request, querytype='task'):
    if not 'view' in request.session['requestParams']:
        request.session['requestParams']['view'] = 'packages'
    query = {}

    # Process time range related params
    if 'ntag_from' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag_from']
        try:
            startdate = datetime.strptime(startdatestr, '%Y-%m-%d')
        except:
            del request.session['requestParams']['ntag_from']

    if 'ntag_to' in request.session['requestParams']:
        enddatestr = request.session['requestParams']['ntag_to']
        try:
            enddate = datetime.strptime(enddatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag_to']

    if 'ntag' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag']
        try:
            startdate = datetime.strptime(startdatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag']
    if 'ntags' in request.session['requestParams']:
        dateliststr = request.session['requestParams']['ntags']
        datelist = []
        for datestr in dateliststr.split(','):
            try:
                datei = datetime.strptime(datestr, artdateformat)
                datelist.append(datei)
            except:
                pass
    if 'ntag_full' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag_full'][:10]
        try:
            startdate = datetime.strptime(startdatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag_full']

    if request.path != '/art/updatejoblist/':
        ndaysmax = 6
    else:
        ndaysmax = 30
    if 'ntag_from' in request.session['requestParams'] and not 'ntag_to' in request.session['requestParams']:
        enddate = startdate + timedelta(days=ndaysmax)
    elif not 'ntag_from' in request.session['requestParams'] and 'ntag_to' in request.session['requestParams']:
        startdate = enddate - timedelta(days=ndaysmax)
    elif not 'ntag_from' in request.session['requestParams'] and not 'ntag_to' in request.session['requestParams']:
        if 'ntag' in request.session['requestParams']:
            enddate = startdate
        elif 'ntag_full' in request.session['requestParams']:
            enddate = startdate
        else:
            enddate = datetime.now()
            startdate = enddate - timedelta(days=ndaysmax)
    elif 'ntag_from' in request.session['requestParams'] and 'ntag_to' in request.session['requestParams'] and (enddate-startdate).days > 7:
        enddate = startdate + timedelta(days=ndaysmax)

    if 'days' in request.session['requestParams']:
        try:
            ndays = int(request.session['requestParams']['days'])
        except:
            ndays = ndaysmax
        enddate = datetime.now()
        if ndays <= ndaysmax:
            startdate = enddate - timedelta(days=ndays)
        else:
            startdate = enddate - timedelta(days=ndaysmax)

    if 'nlastnightlies' in request.session['requestParams']:
        nlastnightlies = int(request.session['requestParams']['nlastnightlies'])
        datelist = find_last_n_nightlies(request, nlastnightlies)
        if len(datelist) == 0:
            startdate = datetime.strptime('2017-05-03', artdateformat)
            enddate = datetime.now()

    if not 'ntag' in request.session['requestParams']:
        if 'ntags' in request.session['requestParams'] or 'nlastnightlies' in request.session['requestParams']:
            if len(datelist) > 0:
                request.session['requestParams']['ntags'] = datelist
                startdate = min(datelist)
                enddate = max(datelist)
            request.session['requestParams']['ntag_from'] = startdate
            request.session['requestParams']['ntag_to'] = enddate
        else:
            request.session['requestParams']['ntag_from'] = startdate
            request.session['requestParams']['ntag_to'] = enddate
    else:
        request.session['requestParams']['ntag'] = startdate

    # Process and prepare a query as a string
    querystr = ''
    if querytype == 'job':
        if 'package' in request.session['requestParams']:
            packages = request.session['requestParams']['package'].split(',')
            if len(packages) == 1 and '*' in packages[0]:
                querystr += preprocessWildCardString(packages[0], 'package').replace('\'', '\'\'') + ' AND '
            else:
                querystr += '(UPPER(PACKAGE) IN ( '
                for p in packages:
                    querystr += 'UPPER(\'\'' + p + '\'\'), '
                if querystr.endswith(', '):
                    querystr = querystr[:len(querystr) - 2]
                querystr += ')) AND '
        if 'branch' in request.session['requestParams']:
            branches = request.session['requestParams']['branch'].split(',')
            querystr += '(UPPER(NIGHTLY_RELEASE_SHORT || \'\'/\'\' || PROJECT || \'\'/\'\' || PLATFORM)  IN ( '
            for b in branches:
                querystr += 'UPPER(\'\'' + b + '\'\'), '
            if querystr.endswith(', '):
                querystr = querystr[:len(querystr) - 2]
            querystr += ')) AND '
        if 'taskid' in request.session['requestParams']:
            querystr += '(a.TASK_ID = ' + request.session['requestParams']['taskid'] + ' ) AND '
        if 'ntags' in request.session['requestParams'] or ('nlastnightlies' in request.session['requestParams'] and len(datelist) > 0):
            querystr += '((SUBSTR(NIGHTLY_TAG_DISPLAY, 0, INSTR(NIGHTLY_TAG_DISPLAY, \'\'T\'\')-1)) IN ('
            for datei in datelist:
                querystr += '\'\'' + datei.strftime(artdateformat) + '\'\', '
            if querystr.endswith(', '):
                querystr = querystr[:len(querystr) - 2]
            querystr += ')) AND '
        if 'ntag_full' in request.session['requestParams']:
            querystr += '(UPPER(NIGHTLY_TAG_DISPLAY) = \'\'' + request.session['requestParams']['ntag_full'] + '\'\') AND'
        if querystr.endswith('AND '):
            querystr = querystr[:len(querystr)-4]
        else:
            querystr += '(1=1)'
        query['strcondition'] = querystr
        query['ntag_from'] = startdate.strftime(artdateformat)
        query['ntag_to'] = enddate.strftime(artdateformat)
    elif querytype == 'task':
        if 'package' in request.session['requestParams'] and not ',' in request.session['requestParams']['package']:
            query['package'] = request.session['requestParams']['package']
        elif 'package' in request.session['requestParams'] and ',' in request.session['requestParams']['package']:
            query['package__in'] = [p for p in request.session['requestParams']['package'].split(',')]
        if 'branch' in request.session['requestParams'] and not ',' in request.session['requestParams']['branch']:
            query['branch'] = request.session['requestParams']['branch']
        elif 'branch' in request.session['requestParams'] and ',' in request.session['requestParams']['branch']:
            query['branch__in'] = [b for b in request.session['requestParams']['branch'].split(',')]
        if not 'ntags' in request.session['requestParams']:
            query['ntag__range'] = [startdate.strftime(artdateformat), enddate.strftime(artdateformat)]
        else:
            query['ntag__in'] = [ntag.strftime(artdateformat) for ntag in datelist]

    return query


def find_last_n_nightlies(request, limit=7):
    """
    Find N last nightlies dates in registered tests
    :param request:
    :return: list of ntags
    """
    nquery = {}
    querystr = '(1=1)'
    if 'package' in request.session['requestParams'] and not ',' in request.session['requestParams']['package'] and not '*' in request.session['requestParams']['package']:
        nquery['package'] = request.session['requestParams']['package']
    elif 'package' in request.session['requestParams'] and ',' in request.session['requestParams']['package']:
        nquery['package__in'] = [p for p in request.session['requestParams']['package'].split(',')]
    elif 'package' in request.session['requestParams'] and '*' in request.session['requestParams']['package']:
        querystr += ' AND ' + preprocessWildCardString(request.session['requestParams']['package'], 'package')
    if 'branch' in request.session['requestParams']:
        branches = request.session['requestParams']['branch'].split(',')
        querystr += ' AND (NIGHTLY_RELEASE_SHORT || \'/\' || PROJECT || \'/\' || PLATFORM)  IN ( '
        for b in branches:
            querystr += '(\'' + b + '\'), '
        if querystr.endswith(', '):
            querystr = querystr[:len(querystr) - 2]
        querystr += ')'
    ndates = ARTTests.objects.filter(**nquery).extra(where=[querystr]).annotate(ndate=Substr('nightly_tag_display', 1, 10)).values('ndate').order_by('-ndate').distinct()[:limit]

    datelist = []
    for datestr in ndates:
        try:
            datei = datetime.strptime(datestr['ndate'], artdateformat)
            datelist.append(datei)
        except:
            pass

    return datelist


def getjflag(job):
    """Returns flag if job in finished state"""
    return 1 if job['jobstatus'] in ('finished', 'failed', 'cancelled', 'closed') else 0
