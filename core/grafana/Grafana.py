import re, pytz
from json import loads
from requests import get
from datetime import datetime, timedelta, timezone
from core.grafana.Headers import Headers


class Grafana(object):
    def __init__(self, grafana_proxy='https://monit-grafana.cern.ch/api/datasources/proxy/',
                 database='monit_production_atlasjm'):
        headers = Headers()
        self.grafana_proxy = grafana_proxy
        self.grafana_database = database
        self.grafana_headers = headers.get_headers_api()

    def _get_datasource(self, datasource):
        if datasource == "completed":
            return "8261"
        if datasource == "submitted":
            return "9017"
        if datasource == "running":
            return "9023"
        if datasource == "pending":
            return "9024"
        if datasource == "pledges_last" or datasource == "pledges_sum" or datasource == "pledges_hs06sec":
            return "9267"

    def get_query(self, object):
        query_object = object
        if query_object.endtime == "" and query_object.starttime == "":
            timebefore = timedelta(days=query_object.days)

            endtime = (datetime.now(tz=timezone.utc)).replace(minute=00, hour=00, second=00, microsecond=000)
            starttime = ((endtime - timebefore)).replace(minute=00, hour=00, second=00, microsecond=000)

            startMillisec = int(starttime.strftime("%s")) * 1000
            endMillisec = int(endtime.strftime("%s")) * 1000
        else:
            try:
                startD = datetime.strptime(query_object.starttime, '%d.%m.%Y %H:%M:%S')
                endD = datetime.strptime(query_object.endtime, '%d.%m.%Y %H:%M:%S')
            except:
                try:
                    startD = datetime.strptime(query_object.starttime, '%Y-%m-%d')
                    endD = datetime.strptime(query_object.endtime, '%Y-%m-%d')
                except:
                    endD = (datetime.now(tz=timezone.utc)).replace(minute=00, hour=00, second=00, microsecond=000)
                    startD = ((endD - 1)).replace(minute=00, hour=00, second=00, microsecond=000)
            startMillisec = int(startD.strftime("%s")) * 1000
            endMillisec = int(endD.strftime("%s")) * 1000
        if query_object.table == "pledges_last":
            query_object.agg_func = 'last'
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
           SELECT {0}(value) FROM "pledges" WHERE  ("pledge_type" = 'CPU' AND "real_federation" =~ /^{1}$/ AND "country" =~ /^{2}$/) 
           AND vo = 'atlas' AND time >= {3}ms and time <= {4}ms GROUP BY {5}
               '''.format(query_object.agg_func, dst_federation, dst_country,
                          startMillisec, endMillisec, query_object.grouping)

            query = re.sub(r"([\n ])\1*", r"\1", query).replace('\n', ' ').lstrip().strip()
            query = re.sub(' +', ' ', query)
            return query
        elif query_object.table == "pledges_sum":
            query_object.agg_func = 'sum'
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
                SELECT sum("mean_value") FROM (SELECT {0}("value") as mean_value FROM "pledges" 
                WHERE ("pledge_type" = 'CPU' AND "real_federation" =~ /^{1}$/ AND "country" =~ /^{2}$/) 
                AND vo = 'atlas' AND time >= {3}ms and time <= {4}ms GROUP BY {5} fill(null)) WHERE time >= {3}ms 
                and time <= {4}ms GROUP BY {5} fill(null)
                '''.format(query_object.agg_func, dst_federation, dst_country,
                           startMillisec, endMillisec, query_object.grouping)
            query = re.sub(r"([\n ])\1*", r"\1", query).replace('\n', ' ').lstrip().strip()
            query = re.sub(' +', ' ', query)
            return query
        elif query_object.table == "pledges_hs06sec":
            query_object.agg_func = 'mean'
            coefficient = 3600
            if '.*' not in query_object.dst_country:
                dst_country = '(' + query_object.dst_country + ')'
            else:
                dst_country = query_object.dst_country
            if '.*' not in query_object.dst_federation:
                dst_federation = '(' + query_object.dst_federation + ')'
            else:
                dst_federation = query_object.dst_federation
            if query_object.bin == '1h':
                coefficient = 3600
            elif query_object.bin == '1d':
                coefficient = 3600 * 24
            elif query_object.bin == '7d':
                coefficient = 3600 * 24 * 7
            elif query_object.bin == '30d':
                coefficient = 3600 * 24 * 30
            # if 'time' in query_object.grouping:
            #     query_object.grouping = re.sub('\d+',str(6),query_object.grouping)
            query = \
                '''
                SELECT sum("mean_value")*{0} FROM (SELECT {1}("value") as mean_value FROM "pledges" 
                WHERE ("pledge_type" = 'CPU' AND "real_federation" =~ /^{2}$/ AND "country" =~ /^{3}$/) 
                AND vo = 'atlas' AND time >= {4}ms and time <= {5}ms GROUP BY time(6h),{7} fill(null)) WHERE time >= {4}ms
                and time <= {5}ms GROUP BY {6} fill(previous)
                '''.format(coefficient, query_object.agg_func, dst_federation, dst_country,
                           startMillisec, endMillisec, query_object.grouping, str(query_object.grouping).split(',')[-1])
            query = re.sub(r"([\n ])\1*", r"\1", query).replace('\n', ' ').lstrip().strip()
            query = re.sub(' +', ' ', query)
            return query
        else:
            if '.*' not in query_object.dst_experiment_site:
                dst_experiment_site = '(' + query_object.dst_experiment_site + ')'
            else:
                dst_experiment_site = query_object.dst_experiment_site
            if '.*' not in query_object.dst_cloud:
                dst_cloud = '(' + query_object.dst_cloud + ')'
            else:
                dst_cloud = query_object.dst_cloud
            if '.*' not in query_object.dst_country:
                dst_country = '(' + query_object.dst_country + ')'
            else:
                dst_country = query_object.dst_country
            if '.*' not in query_object.dst_federation:
                dst_federation = '(' + query_object.dst_federation + ')'
            else:
                dst_federation = query_object.dst_federation
            if '.*' not in query_object.adcactivity:
                adcactivity = '(' + query_object.adcactivity + ')'
            else:
                adcactivity = query_object.adcactivity
            if '.*' not in query_object.resourcesreporting:
                resourcesreporting = '(' + query_object.resourcesreporting + ')'
            else:
                resourcesreporting = query_object.resourcesreporting
            if '.*' not in query_object.actualcorecount:
                actualcorecount = '(' + query_object.actualcorecount + ')'
            else:
                actualcorecount = query_object.actualcorecount
            if '.*' not in query_object.resource_type:
                resource_type = '(' + query_object.resource_type + ')'
            else:
                resource_type = query_object.resource_type
            if '.*' not in query_object.workinggroup:
                workinggroup = '(' + query_object.workinggroup + ')'
            else:
                workinggroup = query_object.workinggroup
            if '.*' not in query_object.inputfiletype:
                inputfiletype = '(' + query_object.inputfiletype + ')'
            else:
                inputfiletype = query_object.inputfiletype
            if '.*' not in query_object.eventservice:
                eventservice = '(' + query_object.eventservice + ')'
            else:
                eventservice = query_object.eventservice
            if '.*' not in query_object.inputfileproject:
                inputfileproject = '(' + query_object.inputfileproject + ')'
            else:
                inputfileproject = query_object.inputfileproject
            if '.*' not in query_object.outputproject:
                outputproject = '(' + query_object.outputproject + ')'
            else:
                outputproject = query_object.outputproject
            if '.*' not in query_object.jobstatus:
                jobstatus = '(' + query_object.jobstatus + ')'
            else:
                jobstatus = query_object.jobstatus
            if '.*' not in query_object.computingsite:
                computingsite = '(' + query_object.computingsite + ')'
            else:
                computingsite = query_object.computingsite
            if '.*' not in query_object.gshare:
                gshare = '(' + query_object.gshare + ')'
            else:
                gshare = query_object.gshare
            if '.*' not in query_object.dst_tier:
                dst_tier = '(' + query_object.dst_tier + ')'
            else:
                dst_tier = query_object.dst_tier
            if '.*' not in query_object.processingtype:
                processingtype = '(' + query_object.processingtype + ')'
            else:
                processingtype = query_object.processingtype
            if '.*' not in query_object.nucleus:
                nucleus = '(' + query_object.nucleus + ')'
            else:
                nucleus = query_object.nucleus
            if '.*' not in query_object.error_category:
                error_category = '(' + query_object.error_category + ')'
            else:
                error_category = query_object.error_category
            if query_object.table == 'pending':
                pquery = """"jobstatus" = 'pending' AND """
            else:
                pquery = ''
            select_con = self.select_con(query_object.agg_func, query_object.field)
            query = \
                '''
            SELECT {0} FROM "long_{2}"."{1}_{2}" 
            WHERE ({26}"dst_experiment_site" =~ /^{3}$/ AND "dst_cloud" =~ /^{4}$/ 
            AND "dst_country" =~ /^{5}$/ AND "dst_federation" =~ /^{6}$/ 
            AND "adcactivity" =~ /^{7}$/ AND "resourcesreporting" =~ /^{8}$/ AND "actualcorecount" =~ /^{9}$/ 
            AND "resource_type" =~ /^{10}$/ AND "workinggroup" =~ /^{11}$/ 
            AND "inputfiletype" =~ /^{12}$/ AND "eventservice" =~ /^{13}$/ 
            AND "inputfileproject" =~ /^{14}$/ AND "outputproject" =~ /^{15}$/ 
            AND "jobstatus" =~ /^{16}$/ AND "computingsite" =~ /^{17}$/ AND "gshare" =~ /^{18}$/ 
            AND "dst_tier" =~ /^{19}$/ AND "processingtype" =~ /^{20}$/ AND "nucleus" =~ /^{21}$/ AND "error_category" =~ /^{22}$/ )  
            AND  time >= {23}ms and time <= {24}ms GROUP BY {25} fill(0)&epoch=ms
            '''.format(select_con, query_object._get_table(query_object.table), query_object.bin, dst_experiment_site,
                       dst_cloud, dst_country,
                       dst_federation, adcactivity, resourcesreporting, actualcorecount, resource_type, workinggroup,
                       inputfiletype, eventservice, inputfileproject, outputproject, jobstatus, computingsite, gshare,
                       dst_tier, processingtype, nucleus, error_category, startMillisec, endMillisec,
                       query_object.grouping, pquery)
            query = re.sub(r"([\n ])\1*", r"\1", query).replace('\n', ' ').lstrip().strip()
            query = re.sub(' +', ' ', query)
            return query

    def get_url(self, query):

        url = self.grafana_proxy + self._get_datasource(
            query.table) + '/query?db=' + self.grafana_database + '_' + query.table + '&q=' + self.get_query(query)
        return url

    def get_data(self, query):
        if query.table == 'pledges_last' or query.table == 'pledges_sum' or query.table == 'pledges_hs06sec':
            url = self.grafana_proxy + self._get_datasource(
                query.table) + '/query?db=' + self.grafana_database + '&q=' + self.get_query(query)
        else:
            url = self.grafana_proxy + self._get_datasource(
                query.table) + '/query?db=' + self.grafana_database + '_' + query.table + '&q=' + self.get_query(query)
        r = get(url, headers=self.grafana_headers)
        res = loads(r.text)
        return res

    def print_data(self, res):
        for s in res['results'][0]['series']:
            tg = ''
            for tag in s['tags']:
                tg = s['tags'][tag]
                for value in s['values']:
                    print(tg + ' ' + str(value[0]) + ' ' + str(
                        datetime.utcfromtimestamp(value[0] / 1000.0).strftime('%d-%b-%y %H:%M:%S')) + ' ' + str(
                        value[1]))

    def select_con(self, agg_func, agg_field):
        if type(agg_field) == list:
            final_func = ''
            for field in agg_field:
                start_func = ''
                end_func = '"' + field + '"'
                if '|' in agg_func:
                    aggr_functions = str(agg_func).split("|")
                    for func in aggr_functions:
                        start_func += func + '('
                        end_func += ')'
                    final_func += start_func + end_func
                else:
                    final_func += agg_func + "(" + end_func + ") as +" + field + "+,"
            final_func = final_func[:-1]
            return final_func
        else:
            final_func = ''
            start_func = ''
            end_func = '"' + agg_field + '"'
            if '|' in agg_func:
                aggr_functions = str(agg_func).split("|")
                for func in aggr_functions:
                    start_func += func + '('
                    end_func += ')'
                final_func = start_func + end_func
            else:
                final_func = agg_func + "(" + end_func + ")"
            return final_func