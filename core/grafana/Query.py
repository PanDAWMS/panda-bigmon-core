from datetime import datetime, timedelta

class Query(object):
    def __init__(self, agg_func = 'sum', field = 'sum_count', table = 'submitted', bin ='1h',
                 dst_experiment_site = ".*",
                 dst_cloud  = ".*",
                 dst_country  = ".*",
                 dst_federation  = ".*",
                 adcactivity  = ".*",
                 resourcesreporting  = ".*",
                 actualcorecount  = ".*",
                 resource_type  = ".*",
                 workinggroup  = ".*",
                 inputfiletype  = ".*",
                 eventservice  = ".*",
                 inputfileproject  = ".*",
                 outputproject  = ".*",
                 jobstatus  = ".*",
                 computingsite  = ".*",
                 gshare  = ".*",
                 dst_tier  = ".*",
                 processingtype  = ".*",
                 nucleus  = ".*",
                 starttime = "",
                 endtime = "",
                 grouping = ['adcactivity'],
                 days = 7
                 ):

        self.agg_func = agg_func
        self.field = field
        self.table = table
        self.bin = bin
        self.dst_experiment_site = dst_experiment_site
        self.dst_cloud = dst_cloud
        self.dst_country = dst_country
        self.dst_federation = dst_federation
        self.adcactivity = adcactivity
        self.resourcesreporting = resourcesreporting
        self.actualcorecount = actualcorecount
        self.resource_type = resource_type
        self.workinggroup = workinggroup
        self.inputfiletype = inputfiletype
        self.eventservice = eventservice
        self.inputfileproject = inputfileproject
        self.outputproject = outputproject
        self.jobstatus = jobstatus
        self.computingsite = computingsite
        self.gshare = gshare
        self.dst_tier = dst_tier
        self.processingtype = processingtype
        self.nucleus = nucleus
        self.starttime = starttime
        self.endtime = endtime
        if ',' in grouping:
            newgroups = ''
            groups = grouping.split(',')
            for group in groups:
                if group != 'time':
                    newgroups += '"' + group + '"' + ','
            newgroups = newgroups[:-1]
            self.grouping = newgroups
        else:
            self.grouping = '"' + str(grouping) + '"'
        self.days = days

    def _get_table(self, table):
        if table == "completed":
            return "atlasjm_ended"
        if table == "submitted":
            return "atlasjm_submitted"
        if table == "running":
            return "atlasjm_current"
        if table == "pending":
            return "atlasjm_current"
        if table == "finalising":
            return "atlasjm_finalising"
        if table == "pledges":
            return "pledges"

    def request_to_query(self, request):
        if 'agg_func' in request.session['requestParams']:
            self.agg_func = request.session['requestParams']['agg_func']
        else:
            self.agg_func = 'sum'
        if 'field' in request.session['requestParams']:
            self.field = request.session['requestParams']['field']
        else:
            self.field = 'sum_count'
        if 'table' in request.session['requestParams']:
            self.table = request.session['requestParams']['table']
        else:
            self.table = 'submitted'
        if 'bin' in request.session['requestParams']:
            self.bin = request.session['requestParams']['bin']
        else:
            self.bin = '1h'
        if 'dst_experiment_site' in request.session['requestParams']:
            self.dst_experiment_site = request.session['requestParams']['dst_experiment_site']
        else:
            self.dst_experiment_site = ".*"
        if 'dst_cloud' in request.session['requestParams']:
            self.dst_cloud = request.session['requestParams']['dst_cloud']
        else:
            self.dst_cloud = ".*"
        if 'dst_country' in request.session['requestParams']:
            self.dst_country = request.session['requestParams']['dst_country']
        else:
            self.dst_country = ".*"
        if 'dst_federation' in request.session['requestParams']:
            self.dst_federation = request.session['requestParams']['dst_federation']
        else:
            self.dst_federation = ".*"
        if 'adcactivity' in request.session['requestParams']:
            self.adcactivity = request.session['requestParams']['adcactivity']
        else:
            self.adcactivity = ".*"
        if 'resourcesreporting' in request.session['requestParams']:
            self.resourcesreporting = request.session['requestParams']['resourcesreporting']
        else:
            self.resourcesreporting = ".*"
        if 'actualcorecount' in request.session['requestParams']:
            self.actualcorecount = request.session['requestParams']['actualcorecount']
        else:
            self.actualcorecount = ".*"
        if 'resource_type' in request.session['requestParams']:
            self.resource_type = request.session['requestParams']['resource_type']
        else:
            self.resource_type = ".*"
        if 'workinggroup' in request.session['requestParams']:
            self.workinggroup = request.session['requestParams']['workinggroup']
        else:
            self.workinggroup = ".*"
        if 'inputfiletype' in request.session['requestParams']:
            self.inputfiletype = request.session['requestParams']['inputfiletype']
        else:
            self.inputfiletype = ".*"
        if 'eventservice' in request.session['requestParams']:
            self.eventservice = request.session['requestParams']['eventservice']
        else:
            self.eventservice = ".*"
        if 'inputfileproject' in request.session['requestParams']:
            self.inputfileproject = request.session['requestParams']['inputfileproject']
        else:
            self.inputfileproject = ".*"
        if 'outputproject' in request.session['requestParams']:
            self.outputproject = request.session['requestParams']['outputproject']
        else:
            self.outputproject = ".*"
        if 'jobstatus' in request.session['requestParams']:
            self.jobstatus = request.session['requestParams']['jobstatus']
        else:
            self.jobstatus = ".*"
        if 'computingsite' in request.session['requestParams']:
            self.computingsite = request.session['requestParams']['computingsite']
        else:
            self.computingsite = ".*"
        if 'gshare' in request.session['requestParams']:
            self.gshare = request.session['requestParams']['gshare']
        else:
            self.gshare = ".*"
        if 'dst_tier' in request.session['requestParams']:
            self.dst_tier = request.session['requestParams']['dst_tier']
        else:
            self.dst_tier = ".*"
        if 'processingtype' in request.session['requestParams']:
            self.processingtype = request.session['requestParams']['processingtype']
        else:
            self.processingtype = ".*"
        if 'nucleus' in request.session['requestParams']:
            self.nucleus = request.session['requestParams']['nucleus']
        else:
            self.nucleus = ".*"
        if 'starttime' in request.session['requestParams']:
            self.starttime = request.session['requestParams']['starttime']
        else:
            self.starttime = str(_round_to_hour(datetime.utcnow() - timedelta(days=7)))
        if 'endtime' in request.session['requestParams']:
            self.endtime = request.session['requestParams']['endtime']
        else:
            self.endtime = str(_round_to_hour(datetime.utcnow() - timedelta(hours=1)))
        if 'groupby' in request.session['requestParams']:
            if 'time' in str(request.session['requestParams']['groupby']):
                newgroups = """time({0}), """.format(self.bin)
            else:
                newgroups = ''
            groups = str(request.session['requestParams']['groupby'])
            if ',' in groups:
                groups = groups.split(',')
                for group in groups:
                    if group != 'time':
                        newgroups += '"' + group + '"' + ','
                newgroups = newgroups[:-1]
            else:
                newgroups = '"' + str(request.session['requestParams']['groupby']) +'"'
            self.grouping = newgroups
        else:
            self.grouping ='time(1h), "adcactivity"'
        return self


def _round_to_hour(dt):
    dt_start_of_hour = dt.replace(minute=0, second=0, microsecond=0)
    dt_half_hour = dt.replace(minute=30, second=0, microsecond=0)

    if dt >= dt_half_hour:
        # round up
        dt = dt_start_of_hour + timedelta(hours=1)
    else:
        # round down
        dt = dt_start_of_hour

    return dt.strftime('%d.%m.%Y %H:%M:%S')