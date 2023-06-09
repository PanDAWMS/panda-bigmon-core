from core.libs.error import errorInfo


def buildGoogleFlowDiagram(request, jobs=None, tasks=None):
    # set up google flow diagram
    if tasks is None:
        tasks = []
    if jobs is None:
        jobs = []
    if 'requestParams' not in request.session or 'flow' not in request.session['requestParams']:
        return None

    flowstruct = {}
    if jobs and len(jobs) > 0:
        flowstruct['maxweight'] = len(jobs)
        flowrows = buildGoogleJobFlow(jobs)
    elif len(tasks) > 0:
        flowstruct['maxweight'] = len(tasks)
        flowrows = buildGoogleTaskFlow(request, tasks)
    else:
        return None
    flowstruct['columns'] = [['string', 'From'], ['string', 'To'], ['number', 'Weight']]
    flowstruct['rows'] = flowrows[:3000]
    return flowstruct


def buildGoogleJobFlow(jobs):
    cloudd = {}
    mcpcloudd = {}
    mcpshownd = {}
    errd = {}
    errshownd = {}
    sited = {}
    statd = {}
    errcountd = {}
    sitecountd = {}
    siteshownd = {}
    ptyped = {}
    ptypecountd = {}
    ptypeshownd = {}
    for job in jobs:
        errinfo = errorInfo(job, nchars=40, mode='string')
        jobstatus = job['jobstatus']
        for js in ('finished', 'holding', 'merging', 'running', 'cancelled', 'transferring', 'starting'):
            if jobstatus == js: errinfo = js
        if errinfo not in errcountd: errcountd[errinfo] = 0
        errcountd[errinfo] += 1
        cloud = job['homecloud']
        mcpcloud = job['cloud']
        ptype = job['processingtype']
        if ptype not in ptypecountd: ptypecountd[ptype] = 0
        ptypecountd[ptype] += 1
        site = job['computingsite']
        if site not in sitecountd: sitecountd[site] = 0
        sitecountd[site] += 1

        if cloud not in cloudd:
            cloudd[cloud] = {}
        if site not in cloudd[cloud]:
            cloudd[cloud][site] = 0
        cloudd[cloud][site] += 1

        if mcpcloud not in mcpcloudd:
            mcpcloudd[mcpcloud] = {}
        if cloud not in mcpcloudd[mcpcloud]: mcpcloudd[mcpcloud][cloud] = 0
        mcpcloudd[mcpcloud][cloud] += 1

        if jobstatus not in errd: errd[jobstatus] = {}
        if errinfo not in errd[jobstatus]: errd[jobstatus][errinfo] = 0
        errd[jobstatus][errinfo] += 1

        if site not in sited:
            sited[site] = {}
        if errinfo not in sited[site]:
            sited[site][errinfo] = 0
        sited[site][errinfo] += 1

        if jobstatus not in statd: statd[jobstatus] = {}
        if errinfo not in statd[jobstatus]:
            statd[jobstatus][errinfo] = 0
        statd[jobstatus][errinfo] += 1

        if ptype not in ptyped: ptyped[ptype] = {}
        if errinfo not in ptyped[ptype]:
            ptyped[ptype][errinfo] = 0
        ptyped[ptype][errinfo] += 1

    flowrows = []
    for mcpcloud in mcpcloudd:
        for cloud in mcpcloudd[mcpcloud]:
            n = mcpcloudd[mcpcloud][cloud]
            if float(n) / len(jobs) > 0.0:
                mcpshownd[mcpcloud] = 1
                flowrows.append(["%s MCP" % mcpcloud, cloud, n])

    othersited = {}
    othersiteErrd = {}
    for cloud in cloudd:
        if cloud not in mcpshownd: continue
        for e in cloudd[cloud]:
            n = cloudd[cloud][e]
            if float(sitecountd[e]) / len(jobs) > .01:
                siteshownd[e] = 1
                flowrows.append([cloud, e, n])
            else:
                flowrows.append([cloud, 'Other sites', n])
                othersited[e] = n
    # for jobstatus in errd:
    #    for errinfo in errd[jobstatus]:
    #        flowrows.append( [ errinfo, jobstatus, errd[jobstatus][errinfo] ] )
    for e in errcountd:
        if float(errcountd[e]) / len(jobs) > .01:
            errshownd[e] = 1

    for site in sited:
        nother = 0
        for e in sited[site]:
            n = sited[site][e]
            if site in siteshownd:
                sitename = site
            else:
                sitename = "Other sites"
            if e in errshownd:
                errname = e
            else:
                errname = 'Other errors'
            flowrows.append([sitename, errname, n])
            if errname not in othersiteErrd: othersiteErrd[errname] = 0
            othersiteErrd[errname] += n

    # for e in othersiteErrd:
    #    if e in errshownd:
    #        flowrows.append( [ 'Other sites', e, othersiteErrd[e] ] )

    for ptype in ptyped:
        if float(ptypecountd[ptype]) / len(jobs) > .05:
            ptypeshownd[ptype] = 1
            ptname = ptype
        else:
            ptname = "Other processing types"
        for e in ptyped[ptype]:
            n = ptyped[ptype][e]
            if e in errshownd:
                flowrows.append([e, ptname, n])
            else:
                flowrows.append(['Other errors', ptname, n])

    return flowrows


def buildGoogleTaskFlow(request, tasks):
    analysis = False
    if 'requestParams' in request.session:
        analysis = 'tasktype' in request.session['requestParams'] and request.session['requestParams'][
            'tasktype'].startswith('anal')
    ptyped = {}
    reqd = {}
    statd = {}
    substatd = {}
    trfd = {}
    filestatd = {}
    cloudd = {}
    reqsized = {}
    reqokd = {}
    ## count the reqid's. Use only the biggest (in file count) if too many.
    for task in tasks:
        if not analysis and 'deftreqid' not in task: continue
        req = int(task['reqid'])
        dsinfo = task['dsinfo']
        nfiles = dsinfo['nfiles']
        if req not in reqsized: reqsized[req] = 0
        reqsized[req] += nfiles
        ## Veto requests that are all done etc.
        if task['superstatus'] != 'done': reqokd[req] = 1

    if not analysis:
        for req in reqsized:
            # de-prioritize requests not specifically OK'd for inclusion
            if req not in reqokd: reqsized[req] = 0

        nmaxreq = 10
        if len(reqsized) > nmaxreq:
            reqkeys = reqsized.keys()
            reqsortl = sorted(reqkeys, key=reqsized.__getitem__, reverse=True)
            reqsortl = reqsortl[:nmaxreq - 1]
        else:
            reqsortl = reqsized.keys()

    for task in tasks:
        ptype = task['processingtype']
        # if 'jedireqid' not in task: continue
        req = int(task['reqid'])
        if not analysis and req not in reqsortl: continue
        stat = task['superstatus']
        substat = task['status']
        # trf = task['transpath']
        trf = task['taskname']
        cloud = task['cloud']
        if cloud == '': cloud = 'No cloud assigned'
        dsinfo = task['dsinfo']
        nfailed = dsinfo['nfilesfailed']
        nfinished = dsinfo['nfilesfinished']
        nfiles = dsinfo['nfiles']
        npending = nfiles - nfailed - nfinished

        if ptype not in ptyped:
            ptyped[ptype] = {}
        if req not in ptyped[ptype]: ptyped[ptype][req] = 0
        ptyped[ptype][req] += nfiles

        if req not in reqd: reqd[req] = {}
        if stat not in reqd[req]:
            reqd[req][stat] = 0
        reqd[req][stat] += nfiles

        if trf not in trfd:
            trfd[trf] = {}
        if stat not in trfd[trf]:
            trfd[trf][stat] = 0
        trfd[trf][stat] += nfiles

        if stat not in statd: statd[stat] = {}
        if substat not in statd[stat]: statd[stat][substat] = 0
        statd[stat][substat] += nfiles

        if substat not in substatd:
            substatd[substat] = {}
        if 'finished' not in substatd[substat]:
            for filestat in ('finished', 'failed', 'pending'):
                substatd[substat][filestat] = 0
        substatd[substat]['finished'] += nfinished
        substatd[substat]['failed'] += nfailed
        substatd[substat]['pending'] += npending

        if cloud not in cloudd:
            cloudd[cloud] = {}
        if 'finished' not in cloudd[cloud]:
            for filestat in ('finished', 'failed', 'pending'):
                cloudd[cloud][filestat] = 0
        cloudd[cloud]['finished'] += nfinished
        cloudd[cloud]['failed'] += nfailed
        cloudd[cloud]['pending'] += npending

    flowrows = []

    if analysis:
        ## Don't include request, task for analysis
        for trf in trfd:
            for stat in trfd[trf]:
                n = trfd[trf][stat]
                flowrows.append([trf, 'Task %s' % stat, n])
    else:
        for ptype in ptyped:
            for req in ptyped[ptype]:
                n = ptyped[ptype][req]
                flowrows.append([ptype, 'Request %s' % req, n])

        for req in reqd:
            for stat in reqd[req]:
                n = reqd[req][stat]
                flowrows.append(['Request %s' % req, 'Task %s' % stat, n])

    for stat in statd:
        for substat in statd[stat]:
            n = statd[stat][substat]
            flowrows.append(['Task %s' % stat, 'Substatus %s' % substat, n])

    for substat in substatd:
        for filestat in substatd[substat]:
            if filestat not in substatd[substat]: continue
            n = substatd[substat][filestat]
            flowrows.append(['Substatus %s' % substat, 'File status %s' % filestat, n])

    for cloud in cloudd:
        for filestat in cloudd[cloud]:
            if filestat not in cloudd[cloud]: continue
            n = cloudd[cloud][filestat]
            flowrows.append(['File status %s' % filestat, cloud, n])

    return flowrows
