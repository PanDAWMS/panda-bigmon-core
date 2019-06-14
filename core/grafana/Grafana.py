import re, pytz
from json import loads
from requests import get
from datetime import datetime, timedelta
from core.grafana.Headers import Headers

class Grafana(object):
    def __init__(self, grafana_proxy = 'https://monit-grafana.cern.ch/api/datasources/proxy/', database='monit_production_atlasjm'):
        headers = Headers()
        self.grafana_proxy = grafana_proxy
        self.grafana_database = database
        self.grafana_headers = headers.get_headers_api()

    def _get_datsource(self, datasource):
        if datasource == "completed":
            return "8261"
        if datasource == "submitted":
            return "9017"
        if datasource == "running":
            return "9023"
        if datasource == "pending":
            return "9024"
        if datasource == "pledges":
            return "8261"

    def get_query(self, object):
        query_object = object
        if query_object.endtime == "" and query_object.starttime == "":
            oneday = timedelta(days=1)
            timebefore = timedelta(days=query_object.days)

            endtime = (datetime.now()).replace(minute=00, hour=00, second=00, microsecond=000)
            starttime = ((endtime - timebefore) - oneday).replace(minute=00, hour=00, second=00, microsecond=000)

            startMillisec = int(starttime.strftime("%s")) * 1000
            endMillisec = int(endtime.strftime("%s")) * 1000
        else:
            startD = datetime.strptime(query_object.starttime, '%d.%m.%Y %H:%M:%S')
            endD = datetime.strptime(query_object.endtime, '%d.%m.%Y %H:%M:%S')

            localtime = pytz.timezone('Europe/Zurich')
            a = localtime.localize(startD)
            dst = bool(a.dst())
            if dst:
                tdelta = timedelta(hours=2)
                startD = startD + tdelta
                endD = endD + tdelta
            else:
                tdelta = timedelta(hours=1)
                startD = startD + tdelta
                endD = endD + tdelta

            startMillisec =  int(startD.strftime("%s")) * 1000

            endMillisec = int(endD.strftime("%s")) * 1000
        if query_object.table == "pledges":
            if '.*' not in query_object.dst_country:
                dst_country = '(' + query_object.dst_country + ')'
            else:
                dst_country = query_object.dst_country
            if '.*' not in query_object.dst_federation:
                dst_federation = '(' + query_object.dst_federation + ')'
            else:
                dst_federation = query_object.dst_federation
            query = \
                '''
                SELECT {0}("atlas") FROM "long_1d"."pledges" 
                WHERE ("pledge_type" = 'CPU' AND "real_federation" =~ /^{1}/ AND "country" =~ /^{2}/)  
                AND time >= {3}ms and time <= {4}ms GROUP BY {5}
                '''.format(query_object.agg_func, dst_federation, dst_country,
                           startMillisec, endMillisec, query_object.grouping)
            query = re.sub(r"([\n ])\1*", r"\1", query).replace('\n', ' ').lstrip().strip()
            query = re.sub(' +', ' ', query)
            return query
        else:
            if '.*' not in query_object.dst_experiment_site:
                dst_experiment_site = '('+query_object.dst_experiment_site+')'
            else: dst_experiment_site = query_object.dst_experiment_site
            if '.*' not in query_object.dst_cloud:
                dst_cloud = '('+query_object.dst_cloud+')'
            else:
                dst_cloud = query_object.dst_cloud
            if '.*' not in query_object.dst_country:
                dst_country = '('+query_object.dst_country+')'
            else:
                dst_country = query_object.dst_country
            if '.*' not in query_object.dst_federation:
                dst_federation = '('+query_object.dst_federation+')'
            else:
                dst_federation = query_object.dst_federation
            if '.*' not in query_object.adcactivity:
                adcactivity = '('+query_object.adcactivity+')'
            else:
                adcactivity = query_object.adcactivity
            if '.*' not in query_object.resourcesreporting:
                resourcesreporting = '('+query_object.resourcesreporting+')'
            else:
                resourcesreporting = query_object.resourcesreporting
            if '.*' not in query_object.actualcorecount:
                actualcorecount = '('+query_object.actualcorecount+')'
            else:
                actualcorecount = query_object.actualcorecount
            if '.*' not in query_object.resource_type:
                resource_type = '('+query_object.resource_type+')'
            else:
                resource_type = query_object.resource_type
            if '.*' not in query_object.workinggroup:
                workinggroup = '('+query_object.workinggroup+')'
            else:
                workinggroup = query_object.workinggroup
            if '.*' not in query_object.inputfiletype:
                inputfiletype = '('+query_object.inputfiletype+')'
            else:
                inputfiletype = query_object.inputfiletype
            if '.*' not in query_object.eventservice:
                eventservice = '('+query_object.eventservice+')'
            else:
                eventservice = query_object.eventservice
            if '.*' not in query_object.inputfileproject:
                inputfileproject = '('+query_object.inputfileproject+')'
            else:
                inputfileproject = query_object.inputfileproject
            if '.*' not in query_object.outputproject:
                outputproject = '('+query_object.outputproject+')'
            else:
                outputproject = query_object.outputproject
            if '.*' not in query_object.jobstatus:
                jobstatus = '('+query_object.jobstatus+')'
            else:
                jobstatus = query_object.jobstatus
            if '.*' not in query_object.computingsite:
                computingsite = '('+query_object.computingsite+')'
            else:
                computingsite = query_object.computingsite
            if '.*' not in query_object.gshare:
                gshare = '('+query_object.gshare+')'
            else:
                gshare = query_object.gshare
            if '.*' not in query_object.dst_tier:
                dst_tier = '('+query_object.dst_tier+')'
            else:
                dst_tier = query_object.dst_tier
            if '.*' not in query_object.processingtype:
                processingtype = '('+query_object.processingtype+')'
            else:
                processingtype = query_object.processingtype
            if '.*' not in query_object.nucleus:
                nucleus = '('+query_object.nucleus+')'
            else:
                nucleus = query_object.nucleus
            if query_object.table == 'pending':
                pquery = """"jobstatus" = 'pending' AND """
            else: pquery = ''

            query =  \
            '''
            SELECT {0}("{1}") FROM "long_{3}"."{2}_{3}" 
            WHERE ({26}"dst_experiment_site" =~ /^{4}$/ AND "dst_cloud" =~ /^{5}$/ 
            AND "dst_country" =~ /^{6}$/ AND "dst_federation" =~ /^{7}$/ 
            AND "adcactivity" =~ /^{8}$/ AND "resourcesreporting" =~ /^{9}$/ AND "actualcorecount" =~ /^{10}$/ 
            AND "resource_type" =~ /^{11}$/ AND "workinggroup" =~ /^{12}$/ 
            AND "inputfiletype" =~ /^{13}$/ AND "eventservice" =~ /^{14}$/ 
            AND "inputfileproject" =~ /^{15}$/ AND "outputproject" =~ /^{16}$/ 
            AND "jobstatus" =~ /^{17}$/ AND "computingsite" =~ /^{18}$/ AND "gshare" =~ /^{19}$/ 
            AND "dst_tier" =~ /^{20}$/ AND "processingtype" =~ /^{21}$/ AND "nucleus" =~ /^{22}$/ )  
            AND  time >= {23}ms and time <= {24}ms GROUP BY {25} fill(0)&epoch=ms
            '''.format(query_object.agg_func, query_object.field, query_object._get_table(query_object.table), query_object.bin, dst_experiment_site, dst_cloud, dst_country,
                    dst_federation, adcactivity, resourcesreporting, actualcorecount, resource_type, workinggroup,
                    inputfiletype, eventservice, inputfileproject, outputproject, jobstatus, computingsite, gshare,
                    dst_tier, processingtype, nucleus, startMillisec, endMillisec, query_object.grouping, pquery)
            query = re.sub(r"([\n ])\1*", r"\1", query).replace('\n', ' ').lstrip().strip()
            query = re.sub(' +', ' ', query)
            return query

    def get_url(self, query):
        
        url = self.grafana_proxy + self._get_datsource(query.table) + '/query?db=' + self.grafana_database+'_'+ query.table + '&q=' + self.get_query(query)
        return url

    def get_data(self, query):
        url = self.grafana_proxy + self._get_datsource(query.table)+'/query?db='+self.grafana_database+'_'+ query.table+'&q='+ self.get_query(query)
        if query.table == 'pledges':
            url = self.grafana_proxy + self._get_datsource(
                query.table) + '/query?db=' + self.grafana_database + '_completed'  + '&q=' + self.get_query(query)
        else:
            url = self.grafana_proxy + self._get_datsource(
                query.table) + '/query?db=' + self.grafana_database + '_' + query.table + '&q=' + self.get_query(query)
        r = get(url, headers=self.grafana_headers)
        res = loads(r.text)
        return res

    def print_data(self, res):
        for s in res['results'][0]['series']:
            tg = ''
            for tag in s['tags']:
                tg =  s['tags'][tag]
                for value in s['values']:
                    print (tg+' '+ str(value[0]) + ' ' + str(datetime.utcfromtimestamp(value[0] / 1000.0).strftime('%d-%b-%y %H:%M:%S'))+ ' ' + str(value[1]))
