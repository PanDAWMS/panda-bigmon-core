
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