
def generate_requests_summary(requests):
    fields_to_aggregate = ['status']
    agg_dict = {}
    for request in requests:
        for field in fields_to_aggregate:
            agg_dict[request[field]] = agg_dict.get(request[field], 0) + 1
    return agg_dict

def parse_request(request):
    retdict = {}
    status = request.session['requestParams'].get('reqstatus', None)
    if status:
        status = status.strip()
        retdict['reqstatus'] = status
    return retdict

def checkIddsTask(taskinfo):
    taskinfo['idds'] = 0

    if taskinfo['splitrule']:
        splitrule = str(taskinfo['splitrule']).split(',')
        if 'HPO=1' in splitrule:
            taskinfo['idds'] = 1
    if taskinfo['processingtype']:
        if 'hpo' in taskinfo['processingtype']:
            taskinfo['idds'] = 1
    if taskinfo['tasktype']:
        if taskinfo['tasktype'] == "prod":
            taskinfo['idds'] = 1