"""
A set of common functions for initializing and setup views
"""

from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

from core.libs.self_monitor import init_self_monitor


def login_customrequired(function):
    def wrap(request, *args, **kwargs):

        # we check here if it is a crawler:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for and x_forwarded_for in notcachedRemoteAddress:
            return function(request, *args, **kwargs)

        if request.user.is_authenticated or (('HTTP_ACCEPT' in request.META) and (
                request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or ('json' in request.GET):
            return function(request, *args, **kwargs)
        else:
            # if '/user/' in request.path:
            #     return HttpResponseRedirect('/login/?next=' + request.get_full_path())
            # else:
            # return function(request, *args, **kwargs)
            return HttpResponseRedirect('/login/?next=' + request.get_full_path())

    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap


def init_request(request, callselfmon=True):
    global VOMODE, ENV, hostname
    ENV = {}
    VOMODE = ''
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
        # VOMODE = 'devtest'
    request.session['IS_TESTER'] = False

    if VOMODE == 'atlas':
        if "MELLON_SAML_RESPONSE" in request.META and base64.b64decode(request.META['MELLON_SAML_RESPONSE']):
            if "ADFS_FULLNAME" in request.META:
                request.session['ADFS_FULLNAME'] = request.META['ADFS_FULLNAME']
            if "ADFS_EMAIL" in request.META:
                request.session['ADFS_EMAIL'] = request.META['ADFS_EMAIL']
            if "ADFS_FIRSTNAME" in request.META:
                request.session['ADFS_FIRSTNAME'] = request.META['ADFS_FIRSTNAME']
            if "ADFS_LASTNAME" in request.META:
                request.session['ADFS_LASTNAME'] = request.META['ADFS_LASTNAME']
            if "ADFS_LOGIN" in request.META:
                request.session['ADFS_LOGIN'] = request.META['ADFS_LOGIN']
                user = None
                try:
                    user = BPUser.objects.get(username=request.session['ADFS_LOGIN'])
                    request.session['IS_TESTER'] = user.is_tester
                    request.session['USER_ID'] = user.id
                except BPUser.DoesNotExist:
                    user = BPUser.objects.create_user(username=request.session['ADFS_LOGIN'],
                                                      email=request.session['ADFS_EMAIL'],
                                                      first_name=request.session['ADFS_FIRSTNAME'],
                                                      last_name=request.session['ADFS_LASTNAME'])
                    user.set_unusable_password()
                    user.save()

    # if VOMODE == 'devtest':
    #     request.session['ADFS_FULLNAME'] = ''
    #     request.session['ADFS_EMAIL'] = ''
    #     request.session['ADFS_FIRSTNAME'] = ''
    #     request.session['ADFS_LASTNAME'] = ''
    #     request.session['ADFS_LOGIN'] = 'tkorchug'
    #     # user = None
    #     user = BPUser.objects.get(username=request.session['ADFS_LOGIN'])
    #     request.session['IS_TESTER'] = user.is_tester

    # print("IP Address for debug-toolbar: " + request.META['REMOTE_ADDR'])

    viewParams = {}
    # if not 'viewParams' in request.session:
    request.session['viewParams'] = viewParams

    url = request.get_full_path()
    u = urlparse(url)
    query = parse_qs(u.query)
    query.pop('timestamp', None)
    try:
        u = u._replace(query=urlencode(query, True))
    except UnicodeEncodeError:
        data = {
            'errormessage': 'Error appeared while encoding URL!'
        }
        return False, render_to_response('errorPage.html', data, content_type='text/html')
    request.session['notimestampurl'] = urlunparse(u) + ('&' if len(query) > 0 else '?')

    request.session['secureurl'] = 'https://bigpanda.cern.ch' + url

    # if 'USER' in os.environ and os.environ['USER'] != 'apache':
    #    request.session['debug'] = True
    if 'debug' in request.GET and request.GET['debug'] == 'insider':
        request.session['debug'] = True
        djangosettings.DEBUG = True
    else:
        request.session['debug'] = False
        djangosettings.DEBUG = False

    if len(hostname) > 0: request.session['hostname'] = hostname

    ##self monitor
    if callselfmon:
        init_self_monitor(request)

    ## Set default page lifetime in the http header, for the use of the front end cache
    request.session['max_age_minutes'] = 10

    ## Is it an https connection with a legit cert presented by the user?
    if 'SSL_CLIENT_S_DN' in request.META or 'HTTP_X_SSL_CLIENT_S_DN' in request.META:
        if 'SSL_CLIENT_S_DN' in request.META:
            request.session['userdn'] = request.META['SSL_CLIENT_S_DN']
        else:
            request.session['userdn'] = request.META['HTTP_X_SSL_CLIENT_S_DN']
        userrec = Users.objects.filter(dn__startswith=request.session['userdn']).values()
        if len(userrec) > 0:
            request.session['username'] = userrec[0]['name']

    ENV['MON_VO'] = ''
    request.session['viewParams']['MON_VO'] = ''
    if 'HTTP_HOST' in request.META:
        for vo in VOLIST:
            if request.META['HTTP_HOST'].startswith(vo):
                VOMODE = vo
    else:
        VOMODE = 'atlas'

    ## If DB is Oracle, set vomode to atlas
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    ENV['MON_VO'] = VONAME[VOMODE]
    request.session['viewParams']['MON_VO'] = ENV['MON_VO']
    global errorFields, errorCodes, errorStages
    requestParams = {}
    request.session['requestParams'] = requestParams

    allowedemptyparams = ('json', 'snap', 'dt', 'dialogs', 'pandaids', 'workersstats')
    if request.method == 'POST':
        for p in request.POST:
            if p in ('csrfmiddlewaretoken',): continue
            pval = request.POST[p]
            pval = pval.replace('+', ' ')
            request.session['requestParams'][p.lower()] = pval
    else:
        for p in request.GET:
            pval = request.GET[p]
            ####if injection###
            if 'script' in pval.lower() or '</' in pval.lower() or '/>' in pval.lower():
                data = {
                    'viewParams': request.session['viewParams'],
                    'requestParams': request.session['requestParams'],
                    "errormessage": "Illegal value '%s' for %s" % (pval, p),
                }
                return False, render_to_response('errorPage.html', data, content_type='text/html')
            pval = pval.replace('+', ' ')
            if p.lower() != 'batchid':  # Special requester exception
                pval = pval.replace('#', '')
            ## is it int, if it's supposed to be?
            if p.lower() in (
                    'days', 'hours', 'limit', 'display_limit', 'taskid', 'jeditaskid', 'jobsetid', 'reqid', 'corecount',
                    'taskpriority',
                    'priority', 'attemptnr', 'statenotupdated', 'tasknotupdated', 'corepower', 'wansourcelimit',
                    'wansinklimit', 'nqueue', 'nodes', 'queuehours', 'memory', 'maxtime', 'space',
                    'maxinputsize', 'timefloor', 'depthboost', 'idlepilotsupression', 'pilotlimit', 'transferringlimit',
                    'cachedse', 'stageinretry', 'stageoutretry', 'maxwdir', 'minmemory', 'maxmemory', 'minrss',
                    'maxrss', 'mintime',):
                try:
                    requestVal = request.GET[p]
                    if '|' in requestVal:
                        values = requestVal.split('|')
                        for value in values:
                            i = int(value)
                    else:
                        i = int(requestVal)
                except:
                    data = {
                        'viewParams': request.session['viewParams'],
                        'requestParams': request.session['requestParams'],
                        "errormessage": "Illegal value '%s' for %s" % (pval, p),
                    }
                    return False, render_to_response('errorPage.html', data, content_type='text/html')
            if p.lower() in ('date_from', 'date_to'):
                try:
                    requestVal = request.GET[p]
                    i = parse_datetime(requestVal)
                except:
                    data = {
                        'viewParams': request.session['viewParams'],
                        'requestParams': request.session['requestParams'],
                        "errormessage": "Illegal value '%s' for %s" % (pval, p),
                    }
                    return False, render_to_response('errorPage.html', data, content_type='text/html')
            if p.lower() not in allowedemptyparams and len(pval) == 0:
                data = {
                    'viewParams': request.session['viewParams'],
                    'requestParams': request.session['requestParams'],
                    "errormessage": "Empty value '%s' for %s" % (pval, p),
                }
                return False, render_to_response('errorPage.html', data, content_type='text/html')
            request.session['requestParams'][p.lower()] = pval
    setupSiteInfo(request)

    with inilock:
        if len(errorFields) == 0:
            codes = ErrorCodes()
            errorFields, errorCodes, errorStages = codes.getErrorCodes()
    return True, None


def preprocessWildCardString(strToProcess, fieldToLookAt):
    if (len(strToProcess) == 0):
        return '(1=1)'
    isNot = False
    if strToProcess.startswith('!'):
        isNot = True
        strToProcess = strToProcess[1:]

    cardParametersRaw = strToProcess.split('*')
    cardRealParameters = [s for s in cardParametersRaw if len(s) >= 1]
    countRealParameters = len(cardRealParameters)
    countParameters = len(cardParametersRaw)

    if (countParameters == 0):
        return '(1=1)'
    currentRealParCount = 0
    currentParCount = 0
    extraQueryString = '('

    for parameter in cardParametersRaw:
        leadStar = False
        trailStar = False
        if len(parameter) > 0:

            if (currentParCount - 1 >= 0):
                #                if len(cardParametersRaw[currentParCount-1]) == 0:
                leadStar = True

            if (currentParCount + 1 < countParameters):
                #                if len(cardParametersRaw[currentParCount+1]) == 0:
                trailStar = True

            if fieldToLookAt.lower() == 'PRODUSERID':
                leadStar = True
                trailStar = True

            if fieldToLookAt.lower() == 'resourcetype':
                fieldToLookAt = 'resource_type'

            isEscape = False
            if '_' in parameter:
                parameter = parameter.replace('_', '!_')
                isEscape = True

            extraQueryString += "(UPPER(" + fieldToLookAt + ") "
            if isNot:
                extraQueryString += "NOT "
            if leadStar and trailStar:
                extraQueryString += " LIKE UPPER('%%" + parameter + "%%')"
            elif not leadStar and not trailStar:
                extraQueryString += " LIKE UPPER('" + parameter + "')"
            elif leadStar and not trailStar:
                extraQueryString += " LIKE UPPER('%%" + parameter + "')"
            elif not leadStar and trailStar:
                extraQueryString += " LIKE UPPER('" + parameter + "%%')"
            if isEscape:
                extraQueryString += " ESCAPE '!'"
            extraQueryString += ")"
            currentRealParCount += 1
            if currentRealParCount < countRealParameters:
                extraQueryString += ' AND '
        currentParCount += 1
    extraQueryString += ')'
    extraQueryString = extraQueryString.replace("%20", " ") if not '%%20' in extraQueryString else extraQueryString
    return extraQueryString


def setup_view(request, opmode='', hours=0, limit=-99, querytype='job', wildCardExt=False):
    viewParams = {}
    if not 'viewParams' in request.session:
        request.session['viewParams'] = viewParams

    LAST_N_HOURS_MAX = 0

    for paramName, paramVal in request.session['requestParams'].items():
        try:
            request.session['requestParams'][paramName] = urllib.unquote(paramVal)
        except:
            request.session['requestParams'][paramName] = paramVal

    excludeJobNameFromWildCard = True
    if 'jobname' in request.session['requestParams']:
        jobrequest = request.session['requestParams']['jobname']
        if (('*' in jobrequest) or ('|' in jobrequest)):
            excludeJobNameFromWildCard = False
    excludeWGFromWildCard = False
    excludeSiteFromWildCard = False
    if 'workinggroup' in request.session['requestParams'] and 'preset' in request.session['requestParams'] and \
            request.session['requestParams']['preset'] == 'MC':
        # if 'workinggroup' in request.session['requestParams']:
        workinggroupQuery = request.session['requestParams']['workinggroup']
        extraQueryString = '('
        for card in workinggroupQuery.split(','):
            if card[0] == '!':
                extraQueryString += ' NOT workinggroup=\'' + escapeInput(card[1:]) + '\' AND'
            else:
                extraQueryString += ' workinggroup=\'' + escapeInput(card[0:]) + '\' OR'
        if extraQueryString.endswith('AND'):
            extraQueryString = extraQueryString[:-3]
        elif extraQueryString.endswith('OR'):
            extraQueryString = extraQueryString[:-2]
        extraQueryString += ')'
        excludeWGFromWildCard = True
    elif 'workinggroup' in request.session['requestParams'] and request.session['requestParams']['workinggroup'] and \
            '*' not in request.session['requestParams']['workinggroup'] and \
            ',' not in request.session['requestParams']['workinggroup']:
        excludeWGFromWildCard = True

    if 'site' in request.session['requestParams'] and (request.session['requestParams']['site'] == 'hpc' or not (
            '*' in request.session['requestParams']['site'] or '|' in request.session['requestParams']['site'])):
        excludeSiteFromWildCard = True

    wildSearchFields = []
    if querytype == 'job':
        for field in Jobsactive4._meta.get_fields():
            if (field.get_internal_type() == 'CharField'):
                if not (field.name == 'jobstatus' or field.name == 'modificationhost'  # or field.name == 'batchid'
                        or (
                                excludeJobNameFromWildCard and field.name == 'jobname')):
                    wildSearchFields.append(field.name)
    if querytype == 'task':
        for field in JediTasks._meta.get_fields():
            if (field.get_internal_type() == 'CharField'):
                if not (field.name == 'modificationhost' or (
                        excludeWGFromWildCard and field.name == 'workinggroup') or (
                                excludeSiteFromWildCard and field.name == 'site')):
                    wildSearchFields.append(field.name)


    if querytype == 'job':
        try:
            extraQueryString += ' AND '
        except NameError:
            extraQueryString = ''
        if 'fileid' in request.session['requestParams'] or 'ecstate' in request.session['requestParams']:
            if 'fileid' in request.session['requestParams'] and request.session['requestParams']['fileid']:
                fileid = request.session['requestParams']['fileid']
            else:
                fileid = None
            if 'datasetid' in request.session['requestParams'] and request.session['requestParams']['datasetid']:
                datasetid = request.session['requestParams']['datasetid']
            else:
                datasetid = None
            if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
                jeditaskid = request.session['requestParams']['jeditaskid']
            else:
                jeditaskid = None
            if 'tk' in request.session['requestParams'] and request.session['requestParams']['tk']:
                tk = request.session['requestParams']['tk']
                del request.session['requestParams']['tk']
            else:
                tk = None

            if jeditaskid and datasetid and fileid:
                extraQueryString += """
                    pandaid in (
                    (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                    union all
                    (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                    ) """.format(jeditaskid, datasetid, fileid, jeditaskid, datasetid, fileid)

            if 'ecstate' in request.session['requestParams'] and tk and datasetid:
                extraQueryString += """
                    pandaid in (
                        (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid in ( {} ) and fileid in (select id from atlas_pandabigmon.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                        union all 
                        (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid in ( {} ) and fileid in (select id from atlas_pandabigmon.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                        ) """.format(jeditaskid, datasetid, tk, jeditaskid, datasetid, tk)
        elif 'jeditaskid' in request.session['requestParams'] and 'datasetid' in request.session['requestParams']:
            fileid = None
            if 'datasetid' in request.session['requestParams'] and request.session['requestParams']['datasetid']:
                datasetid = request.session['requestParams']['datasetid']
            else:
                datasetid = None
            if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
                jeditaskid = request.session['requestParams']['jeditaskid']
            else:
                jeditaskid = None
            if datasetid and jeditaskid:
                extraQueryString += """
                    pandaid in (
                    (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid = {} )
                    union all
                    (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid = {})
                    ) """.format(jeditaskid, datasetid, jeditaskid, datasetid)
        else:
            fileid = None

    deepquery = False
    fields = standard_fields
    if 'limit' in request.session['requestParams']:
        request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
    elif limit != -99 and limit > 0:
        request.session['JOB_LIMIT'] = limit
    elif VOMODE == 'atlas':
        request.session['JOB_LIMIT'] = 10000
    else:
        request.session['JOB_LIMIT'] = 10000

    if VOMODE == 'atlas':
        LAST_N_HOURS_MAX = 12
    else:
        LAST_N_HOURS_MAX = 7 * 24

    if VOMODE == 'atlas':
        if 'cloud' not in fields: fields.append('cloud')
        if 'atlasrelease' not in fields: fields.append('atlasrelease')
        if 'produsername' in request.session['requestParams'] or 'jeditaskid' in request.session[
            'requestParams'] or 'user' in request.session['requestParams']:
            if 'jobsetid' not in fields: fields.append('jobsetid')
            if ('hours' not in request.session['requestParams']) and (
                    'days' not in request.session['requestParams']) and (
                    'jobsetid' in request.session['requestParams'] or 'taskid' in request.session[
                'requestParams'] or 'jeditaskid' in request.session['requestParams']):
                ## Cases where deep query is safe. Unless the time depth is specified in URL.
                if 'hours' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
                    deepquery = True
        else:
            if 'jobsetid' in fields: fields.remove('jobsetid')
    else:
        fields.append('vo')

    if hours > 0:
        ## Call param overrides default hours, but not a param on the URL
        LAST_N_HOURS_MAX = hours
    ## For site-specific queries, allow longer time window

    if 'batchid' in request.session['requestParams'] and (hours is None or hours == 0):
        LAST_N_HOURS_MAX = 12
    if 'computingsite' in request.session['requestParams'] and hours is None:
        LAST_N_HOURS_MAX = 12
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
        LAST_N_HOURS_MAX = 3 * 24
    ## hours specified in the URL takes priority over the above
    if 'hours' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['hours'])
    if 'days' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['days']) * 24
    ## Exempt single-job, single-task etc queries from time constraint
    if 'hours' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
        if 'jeditaskid' in request.session['requestParams']: deepquery = True
        if 'taskid' in request.session['requestParams']: deepquery = True
        if 'pandaid' in request.session['requestParams']: deepquery = True
        if 'jobname' in request.session['requestParams']: deepquery = True
        # if 'batchid' in request.session['requestParams']: deepquery = True
    if deepquery:
        opmode = 'notime'
        hours = LAST_N_HOURS_MAX = 24 * 180
        request.session['JOB_LIMIT'] = 999999
    if opmode != 'notime':
        if LAST_N_HOURS_MAX <= 72 and not (
                'date_from' in request.session['requestParams'] or 'date_to' in request.session['requestParams']
                or 'earlierthan' in request.session['requestParams'] or 'earlierthandays' in request.session[
                    'requestParams']):
            request.session['viewParams']['selection'] = ", last %s hours" % LAST_N_HOURS_MAX
        else:
            request.session['viewParams']['selection'] = ", last %d days" % (float(LAST_N_HOURS_MAX) / 24.)
        # if JOB_LIMIT < 999999 and JOB_LIMIT > 0:
        #    viewParams['selection'] += ", <font style='color:#FF8040; size=-1'>Warning: limit %s per job table</font>" % JOB_LIMIT
        request.session['viewParams']['selection'] += ". <b>Params:</b> "
        # if 'days' not in requestParams:
        #    viewParams['selection'] += "hours=%s" % LAST_N_HOURS_MAX
        # else:
        #    viewParams['selection'] += "days=%s" % int(LAST_N_HOURS_MAX/24)
        if request.session['JOB_LIMIT'] < 100000 and request.session['JOB_LIMIT'] > 0:
            request.session['viewParams']['selection'] += " <b>limit=</b>%s" % request.session['JOB_LIMIT']
    else:
        request.session['viewParams']['selection'] = ""
    for param in request.session['requestParams']:
        if request.session['requestParams'][param] == 'None': continue
        if request.session['requestParams'][param] == '': continue
        if param == 'display_limit': continue
        if param == 'sortby': continue
        if param == 'limit' and request.session['JOB_LIMIT'] > 0: continue
        request.session['viewParams']['selection'] += " <b>%s=</b>%s " % (
            param, request.session['requestParams'][param])

    startdate = None
    if 'date_from' in request.session['requestParams']:
        startdate = parse_datetime(request.session['requestParams']['date_from'])
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    # startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = None
    if 'date_to' in request.session['requestParams']:
        enddate = parse_datetime(request.session['requestParams']['date_to'])
    if 'earlierthan' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthan']))
    # enddate = enddate.strftime(defaultDatetimeFormat)
    if 'earlierthandays' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthandays']) * 24)
    # enddate = enddate.strftime(defaultDatetimeFormat)
    if enddate == None:
        enddate = timezone.now()  # .strftime(defaultDatetimeFormat)
        request.session['noenddate'] = True
    else:
        request.session['noenddate'] = False

    if request.path.startswith('/running'):
        query = {}
    else:
        query = {
            'modificationtime__castdate__range': [startdate.strftime(defaultDatetimeFormat),
                                                  enddate.strftime(defaultDatetimeFormat)]}

    request.session['TFIRST'] = startdate  # startdate[:18]
    request.session['TLAST'] = enddate  # enddate[:18]

    ### Add any extensions to the query determined from the URL
    # query['vo'] = 'atlas'
    # for vo in ['atlas', 'core']:
    #    if 'HTTP_HOST' in request.META and request.META['HTTP_HOST'].startswith(vo):
    #        query['vo'] = vo
    for param in request.session['requestParams']:
        if param in ('hours', 'days'): continue
        if param == 'cloud' and request.session['requestParams'][param] == 'All':
            continue
        elif param == 'harvesterinstance' or param == 'harvesterid':
            if request.session['requestParams'][param] == 'all':
                query['schedulerid__startswith'] = 'harvester'
            else:
                val = request.session['requestParams'][param]
                query['schedulerid'] = 'harvester-' + val
        elif param == 'schedulerid':
            if 'harvester-*' in request.session['requestParams'][param]:
                query['schedulerid__startswith'] = 'harvester'
            else:
                val = request.session['requestParams'][param]
                query['schedulerid__startswith'] = val
        elif param == 'priorityrange':
            mat = re.match('([0-9]+)\:([0-9]+)', request.session['requestParams'][param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['currentpriority__gte'] = plo
                query['currentpriority__lte'] = phi
        elif param == 'jobsetrange':
            mat = re.match('([0-9]+)\:([0-9]+)', request.session['requestParams'][param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['jobsetid__gte'] = plo
                query['jobsetid__lte'] = phi
        elif param == 'user' or param == 'username':
            if querytype == 'job':
                query['produsername__icontains'] = request.session['requestParams'][param].strip()
        elif param in ('project',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__istartswith'] = val
        elif param in ('outputfiletype',) and querytype != 'task':
            val = request.session['requestParams'][param]
            query['destinationdblock__icontains'] = val
        elif param in ('stream',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__icontains'] = val

        elif param == 'harvesterid':
            val = escapeInput(request.session['requestParams'][param])
            values = val.split(',')
            query['harvesterid__in'] = values


        elif param in ('tag',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__endswith'] = val


        elif param == 'reqid_from':
            val = int(request.session['requestParams'][param])
            query['reqid__gte'] = val
        elif param == 'reqid_to':
            val = int(request.session['requestParams'][param])
            query['reqid__lte'] = val
        elif param == 'processingtype' and '|' not in request.session['requestParams'][param] and '*' not in \
                request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            query['processingtype'] = val
        elif param == 'mismatchedcloudsite' and request.session['requestParams'][param] == 'true':
            listOfCloudSitesMismatched = cache.get('mismatched-cloud-sites-list')
            if (listOfCloudSitesMismatched is None) or (len(listOfCloudSitesMismatched) == 0):
                request.session['viewParams'][
                    'selection'] += "      <b>The query can not be processed because list of mismatches is not found. Please visit %s/dash/production/?cloudview=region page and then try again</b>" % \
                                    request.session['hostname']
            else:
                try:
                    extraQueryString += ' AND ( '
                except NameError:
                    extraQueryString = '('
                for count, cloudSitePair in enumerate(listOfCloudSitesMismatched):
                    extraQueryString += ' ( (cloud=\'%s\') and (computingsite=\'%s\') ) ' % (
                        cloudSitePair[1], cloudSitePair[0])
                    if (count < (len(listOfCloudSitesMismatched) - 1)):
                        extraQueryString += ' OR '
                extraQueryString += ')'
        elif param == 'pilotversion' and request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            if val == 'Not specified':
                try:
                    extraQueryString += ' AND ( '
                except NameError:
                    extraQueryString = '('
                extraQueryString += '(pilotid not like \'%%|%%\') or (pilotid is null)'
                extraQueryString += ')'
            else:
                query['pilotid__endswith'] = val
        elif param == 'durationmin' and request.session['requestParams'][param]:
            try:
                durationrange = request.session['requestParams'][param].split('-')
            except:
                continue
            try:
                extraQueryString += ' AND ( '
            except NameError:
                extraQueryString = '('
            if durationrange[0] == '0' and durationrange[1] == '0':
                extraQueryString += ' (endtime is NULL and starttime is null) ) '
            else:
                extraQueryString += """ 
            (endtime is not NULL and starttime is not null 
            and (endtime - starttime) * 24 * 60 > {} and (endtime - starttime) * 24 * 60 < {} ) 
            or 
            (endtime is NULL and starttime is not null 
            and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 > {} and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 < {} ) 
            ) """.format(str(durationrange[0]), str(durationrange[1]), str(durationrange[0]), str(durationrange[1]))

        if querytype == 'task':
            for field in JediTasks._meta.get_fields():
                # for param in requestParams:
                if param == field.name:
                    if param == 'ramcount':
                        if 'GB' in request.session['requestParams'][param]:
                            leftlimit, rightlimit = (request.session['requestParams'][param]).split('-')
                            rightlimit = rightlimit[:-2]
                            query['%s__range' % param] = (int(leftlimit) * 1000, int(rightlimit) * 1000 - 1)
                        else:
                            query[param] = int(request.session['requestParams'][param])
                    elif param == 'transpath':
                        query['%s__endswith' % param] = request.session['requestParams'][param]
                    elif param == 'tasktype':
                        ttype = request.session['requestParams'][param]
                        if ttype.startswith('anal'):
                            ttype = 'anal'
                        elif ttype.startswith('prod'):
                            ttype = 'prod'
                        query[param] = ttype
                    elif param == 'jeditaskid':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split('|')
                        query['jeditaskid__in'] = values
                    elif param == 'status':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split(',')
                        query['status__in'] = values
                    elif param == 'superstatus':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split('|')
                        query['superstatus__in'] = values
                    elif param == 'workinggroup':
                        if request.session['requestParams'][param] and \
                                '*' not in request.session['requestParams'][param] and \
                                ',' not in request.session['requestParams'][param]:
                            query[param] = request.session['requestParams'][param]
                    elif param == 'reqid':
                        val = escapeInput(request.session['requestParams'][param])
                        if val.find('|') >= 0:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query['reqid__in'] = values
                        else:
                            query['reqid'] = int(val)
                    elif param == 'site':
                        if request.session['requestParams'][param] != 'hpc' and excludeSiteFromWildCard:
                            query['site__contains'] = request.session['requestParams'][param]
                    elif param == 'eventservice':
                        if request.session['requestParams'][param] == 'eventservice' or \
                                request.session['requestParams'][param] == '1':
                            query['eventservice'] = 1
                        else:
                            query['eventservice'] = 0
                    else:
                        if (param not in wildSearchFields):
                            query[param] = request.session['requestParams'][param]
        elif param == 'reqtoken':
            data = getCacheData(request, request.session['requestParams']['reqtoken'])
            if data is not None:
                if 'pandaid' in data:
                    pid = data['pandaid']
                    if pid.find(',') >= 0:
                        pidl = pid.split(',')
                        query['pandaid__in'] = pidl
                    else:
                        query['pandaid'] = int(pid)
                elif 'jeditaskid' in data:
                    tid = data['jeditaskid']
                    if tid.find(',') >= 0:
                        tidl = tid.split(',')
                        query['jeditaskid__in'] = tidl
                    else:
                        query['jeditaskid'] = int(tid)

            else:
                return 'reqtoken', None, None

        else:
            for field in Jobsactive4._meta.get_fields():
                if param == field.name:
                    if param == 'minramcount':
                        if 'GB' in request.session['requestParams'][param]:
                            leftlimit, rightlimit = (request.session['requestParams'][param]).split('-')
                            rightlimit = rightlimit[:-2]
                            query['%s__range' % param] = (int(leftlimit) * 1000, int(rightlimit) * 1000 - 1)
                        else:
                            query[param] = int(request.session['requestParams'][param])
                    elif param == 'specialhandling' and not '*' in request.session['requestParams'][param]:
                        query['specialhandling__contains'] = request.session['requestParams'][param]
                    elif param == 'reqid':
                        val = escapeInput(request.session['requestParams'][param])
                        if val.find('|') >= 0:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query['reqid__in'] = values
                        else:
                            query['reqid'] = int(val)
                    elif param == 'transformation' or param == 'transpath':
                        paramQuery = request.session['requestParams'][param]
                        if paramQuery[0] == '*': paramQuery = paramQuery[1:]
                        if paramQuery[-1] == '*': paramQuery = paramQuery[:-1]
                        query['%s__contains' % param] = paramQuery
                    elif param == 'modificationhost' and request.session['requestParams'][param].find('@') < 0:
                        paramQuery = request.session['requestParams'][param]
                        if paramQuery[0] == '*': paramQuery = paramQuery[1:]
                        if paramQuery[-1] == '*': paramQuery = paramQuery[:-1]
                        query['%s__contains' % param] = paramQuery
                    elif param == 'jeditaskid':
                        if request.session['requestParams']['jeditaskid'] != 'None':
                            if int(request.session['requestParams']['jeditaskid']) < 4000000:
                                query['taskid'] = request.session['requestParams'][param]
                            else:
                                query[param] = request.session['requestParams'][param]
                    elif param == 'taskid':
                        if request.session['requestParams']['taskid'] != 'None': query[param] = \
                            request.session['requestParams'][param]
                    elif param == 'pandaid':
                        try:
                            pid = request.session['requestParams']['pandaid']
                            if pid.find(',') >= 0:
                                pidl = pid.split(',')
                                query['pandaid__in'] = pidl
                            else:
                                query['pandaid'] = int(pid)
                        except:
                            query['jobname'] = request.session['requestParams']['pandaid']
                    elif param == 'jobstatus' and request.session['requestParams'][param] == 'finished' \
                            and (('mode' in request.session['requestParams'] and request.session['requestParams'][
                        'mode'] == 'eventservice') or (
                                         'jobtype' in request.session['requestParams'] and
                                         request.session['requestParams'][
                                             'jobtype'] == 'eventservice')):
                        query['jobstatus__in'] = ('finished', 'cancelled')
                    elif param == 'jobstatus':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split('|')
                        query['jobstatus__in'] = values
                    elif param == 'eventservice':
                        if '|' in request.session['requestParams'][param]:
                            paramsstr = request.session['requestParams'][param]
                            paramsstr = paramsstr.replace('eventservice', '1')
                            paramsstr = paramsstr.replace('esmerge', '2')
                            paramsstr = paramsstr.replace('clone', '3')
                            paramsstr = paramsstr.replace('cojumbo', '5')
                            paramsstr = paramsstr.replace('jumbo', '4')
                            paramvalues = paramsstr.split('|')
                            try:
                                paramvalues = [int(p) for p in paramvalues]
                            except:
                                paramvalues = []
                            query['eventservice__in'] = paramvalues
                        else:
                            if request.session['requestParams'][param] == 'esmerge' or request.session['requestParams'][
                                param] == '2':
                                query['eventservice'] = 2
                            elif request.session['requestParams'][param] == 'clone' or request.session['requestParams'][
                                param] == '3':
                                query['eventservice'] = 3
                            elif request.session['requestParams'][param] == 'jumbo' or request.session['requestParams'][
                                param] == '4':
                                query['eventservice'] = 4
                            elif request.session['requestParams'][param] == 'cojumbo' or \
                                    request.session['requestParams'][
                                        param] == '5':
                                query['eventservice'] = 5
                            elif request.session['requestParams'][param] == 'eventservice' or \
                                    request.session['requestParams'][param] == '1':
                                query['eventservice'] = 1
                                try:
                                    extraQueryString += " not specialhandling like \'%%sc:%%\' "
                                except NameError:
                                    extraQueryString = " not specialhandling like \'%%sc:%%\' "

                            elif request.session['requestParams'][param] == 'not2':
                                try:
                                    extraQueryString += ' AND (eventservice != 2) '
                                except NameError:
                                    extraQueryString = '(eventservice != 2)'
                            else:
                                query['eventservice__isnull'] = True
                    elif param == 'corecount' and request.session['requestParams'][param] == '1':
                        try:
                            extraQueryString += 'AND (corecount = 1 or corecount is NULL)'
                        except:
                            extraQueryString = '(corecount = 1 or corecount is NULL) '
                    else:
                        if (param not in wildSearchFields):
                            query[param] = request.session['requestParams'][param]

    if 'jobtype' in request.session['requestParams']:
        jobtype = request.session['requestParams']['jobtype']
    else:
        jobtype = opmode
    if jobtype.startswith('anal'):
        query['prodsourcelabel__in'] = ['panda', 'user', 'prod_test', 'rc_test']
    elif jobtype.startswith('prod'):
        query['prodsourcelabel__in'] = ['managed', 'prod_test', 'rc_test']
    elif jobtype == 'groupproduction':
        query['prodsourcelabel'] = 'managed'
        query['workinggroup__isnull'] = False
    elif jobtype == 'eventservice':
        query['eventservice'] = 1
    elif jobtype == 'esmerge':
        query['eventservice'] = 2
    elif jobtype.find('test') >= 0:
        query['prodsourcelabel__icontains'] = jobtype

    if 'region' in request.session['requestParams']:
        region = request.session['requestParams']['region']
        siteListForRegion = []
        try:
            homeCloud
        except NameError:
            setupSiteInfo(request)
        else:
            setupSiteInfo(request)

        for sn, rn in homeCloud.items():
            if rn == region:
                siteListForRegion.append(str(sn))
        query['computingsite__in'] = siteListForRegion

    if (wildCardExt == False):
        return query

    try:
        extraQueryString += ' AND '
    except NameError:
        extraQueryString = ''

    wildSearchFields = (set(wildSearchFields) & set(list(request.session['requestParams'].keys())))
    wildSearchFields1 = set()
    for currenfField in wildSearchFields:
        if not (currenfField.lower() == 'transformation'):
            if not ((currenfField.lower() == 'cloud') & (
                    any(card.lower() == 'all' for card in request.session['requestParams'][currenfField].split('|')))):
                if not any(currenfField in key for key, value in query.items()):
                    wildSearchFields1.add(currenfField)
    wildSearchFields = wildSearchFields1

    lenWildSearchFields = len(wildSearchFields)
    currentField = 1

    for currenfField in wildSearchFields:
        extraQueryString += '('
        wildCards = request.session['requestParams'][currenfField].split('|')
        countCards = len(wildCards)
        currentCardCount = 1
        if not ((currenfField.lower() == 'cloud') & (any(card.lower() == 'all' for card in wildCards))):
            for card in wildCards:
                extraQueryString += preprocessWildCardString(card, currenfField)
                if (currentCardCount < countCards): extraQueryString += ' OR '
                currentCardCount += 1
            extraQueryString += ')'
            if (currentField < lenWildSearchFields): extraQueryString += ' AND '
            currentField += 1

    if ('jobparam' in request.session['requestParams'].keys()):
        jobParWildCards = request.session['requestParams']['jobparam'].split('|')
        jobParCountCards = len(jobParWildCards)
        jobParCurrentCardCount = 1
        extraJobParCondition = '('
        for card in jobParWildCards:
            extraJobParCondition += preprocessWildCardString(escapeInput(card), 'JOBPARAMETERS')
            if (jobParCurrentCardCount < jobParCountCards): extraJobParCondition += ' OR '
            jobParCurrentCardCount += 1
        extraJobParCondition += ')'

        pandaIDs = []
        jobParamQuery = {'modificationtime__castdate__range': [startdate.strftime(defaultDatetimeFormat),
                                                               enddate.strftime(defaultDatetimeFormat)]}

        jobs = Jobparamstable.objects.filter(**jobParamQuery).extra(where=[extraJobParCondition]).values('pandaid')
        for values in jobs:
            pandaIDs.append(values['pandaid'])

        query['pandaid__in'] = pandaIDs

    if extraQueryString.endswith(' AND '):
        extraQueryString = extraQueryString[:-5]

    if (len(extraQueryString) < 2):
        extraQueryString = '1=1'

    return (query, extraQueryString, LAST_N_HOURS_MAX)
