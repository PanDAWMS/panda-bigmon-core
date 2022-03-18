"""
A set of functions related to handling EventService jobs and tasks
"""


def job_suppression(request):

    extra = '(1=1)'

    if not 'notsuppress' in request.session['requestParams']:
        suppressruntime = 10
        if 'suppressruntime' in request.session['requestParams']:
            try:
                suppressruntime = int(request.session['requestParams']['suppressruntime'])
            except:
                pass
        extra = '( not ((JOBDISPATCHERERRORCODE=100 OR PILOTERRORCODE in (1200,1201,1202,1203,1204,1206,1207))'
        extra += 'and ((ENDTIME-STARTTIME)*24*60 < {} )))'.format(str(suppressruntime))

    return extra
