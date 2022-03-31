from datetime import timedelta, timezone

import core.constants as const


def user_summary_dict(jobs):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    for job in jobs:
        if 'produsername' in job and job['produsername'] is not None:
            user = job['produsername'].lower()
        else:
            user = 'Unknown'
        if not user in sumd:
            sumd[user] = {}
            for state in const.JOB_STATES:
                sumd[user][state] = 0
            sumd[user]['name'] = job['produsername']
            sumd[user]['cputime'] = 0
            sumd[user]['njobs'] = 0
            for state in const.JOB_STATES:
                sumd[user]['n' + state] = 0
            sumd[user]['nsites'] = 0
            sumd[user]['sites'] = {}
            sumd[user]['nclouds'] = 0
            sumd[user]['clouds'] = {}
            sumd[user]['nqueued'] = 0
            sumd[user]['latest'] = timezone.now() - timedelta(hours=2400)
            sumd[user]['pandaid'] = 0
        cloud = job['cloud']
        site = job['computingsite']
        cpu = float(job['cpuconsumptiontime']) / 1.
        state = job['jobstatus']
        if job['modificationtime'] > sumd[user]['latest']:
            sumd[user]['latest'] = job['modificationtime']
        if job['pandaid'] > sumd[user]['pandaid']:
            sumd[user]['pandaid'] = job['pandaid']
        sumd[user]['cputime'] += cpu
        sumd[user]['njobs'] += 1
        if 'n%s' % (state) not in sumd[user]:
            sumd[user]['n' + state] = 0
        sumd[user]['n' + state] += 1
        if not site in sumd[user]['sites']:
            sumd[user]['sites'][site] = 0
        sumd[user]['sites'][site] += 1
        if not cloud in sumd[user]['clouds']:
            sumd[user]['clouds'][cloud] = 0
        sumd[user]['clouds'][cloud] += 1
    for user in sumd:
        sumd[user]['nsites'] = len(sumd[user]['sites'])
        sumd[user]['nclouds'] = len(sumd[user]['clouds'])
        sumd[user]['nqueued'] = sumd[user]['ndefined'] + sumd[user]['nwaiting'] + sumd[user]['nassigned'] + sumd[user]['nactivated']
        sumd[user]['cputime'] = "%d" % float(sumd[user]['cputime'])

    # convert to list ordered by username
    ukeys = list(sumd.keys())
    ukeys = sorted(ukeys)
    suml = []
    for u in ukeys:
        uitem = {}
        uitem['name'] = u
        uitem['latest'] = sumd[u]['pandaid']
        uitem['dict'] = sumd[u]
        suml.append(uitem)
    suml = sorted(suml, key=lambda x: -x['latest'])
    return suml