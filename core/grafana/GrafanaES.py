import re, pytz
from json import loads
from requests import post
from datetime import datetime, timedelta
from core.grafana.Headers import Headers


class Grafana(object):
    def __init__(self, grafana_proxy='https://monit-grafana.cern.ch',
                 ):
        headers = Headers()
        self.grafana_proxy = grafana_proxy
        self.grafana_headers = headers.get_headers_api()

    def _get_datasource(self, table):
        if table == "completed":
            return "9559"
        if table == "submitted":
            return "9569"
        if table == "running":
            return "9543"
        if table == "pending":
            return "9568"

    def _get_index(self, table):
        if table == "completed":
            return "monit_prod_atlasjm_agg_completed"
        if table == "submitted":
            return "monit_prod_atlasjm_agg_submitted"
        if table == "running":
            return "monit_prod_atlasjm_agg_running"
        if table == "pending":
            return "monit_prod_atlasjm_agg_pending"

    def get_query(self, object):
        query_object = object

        if query_object.endtime == "" and query_object.starttime == "":
            timebefore = timedelta(days=query_object.days)

            endtime = (datetime.utcnow()).replace(minute=00, hour=00, second=00, microsecond=000)
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
                    endD = (datetime.utcnow()).replace(minute=00, hour=00, second=00, microsecond=000)
                    startD = ((endD - 1)).replace(minute=00, hour=00, second=00, microsecond=000)
            startMillisec = int(startD.strftime("%s")) * 1000
            endMillisec = int(endD.strftime("%s")) * 1000

        if '*' not in query_object.dst_experiment_site:
            dst_experiment_site = '(' + query_object.dst_experiment_site + ')'
        else:
            dst_experiment_site = query_object.dst_experiment_site
        if '*' not in query_object.dst_cloud:
            dst_cloud = '(' + query_object.dst_cloud + ')'
        else:
            dst_cloud = query_object.dst_cloud
        if '*' not in query_object.dst_country:
            dst_country = '(' + query_object.dst_country + ')'
        else:
            dst_country = query_object.dst_country
        if '*' not in query_object.dst_federation:
            dst_federation = '(' + query_object.dst_federation + ')'
        else:
            dst_federation = query_object.dst_federation
        if '*' not in query_object.adcactivity:
            adcactivity = '(' + query_object.adcactivity + ')'
        else:
            adcactivity = query_object.adcactivity
        if '*' not in query_object.resourcesreporting:
            resourcesreporting = '(' + query_object.resourcesreporting + ')'
        else:
            resourcesreporting = query_object.resourcesreporting
        if '*' not in query_object.actualcorecount:
            actualcorecount = '(' + query_object.actualcorecount + ')'
        else:
            actualcorecount = query_object.actualcorecount
        if '*' not in query_object.resource_type:
            resource_type = '(' + query_object.resource_type + ')'
        else:
            resource_type = query_object.resource_type
        if '*' not in query_object.workinggroup:
            workinggroup = '(' + query_object.workinggroup + ')'
        else:
            workinggroup = query_object.workinggroup
        if '*' not in query_object.inputfiletype:
            inputfiletype = '(' + query_object.inputfiletype + ')'
        else:
            inputfiletype = query_object.inputfiletype
        if '*' not in query_object.eventservice:
            eventservice = '(' + query_object.eventservice + ')'
        else:
            eventservice = query_object.eventservice
        if '*' not in query_object.inputfileproject:
            inputfileproject = '(' + query_object.inputfileproject + ')'
        else:
            inputfileproject = query_object.inputfileproject
        if '*' not in query_object.outputproject:
            outputproject = '(' + query_object.outputproject + ')'
        else:
            outputproject = query_object.outputproject
        if '*' not in query_object.jobstatus:
            jobstatus = '(' + query_object.jobstatus + ')'
        else:
            jobstatus = query_object.jobstatus
        if '*' not in query_object.computingsite:
            computingsite = '(' + query_object.computingsite + ')'
        else:
            computingsite = query_object.computingsite
        if '*' not in query_object.gshare:
            gshare = '(' + query_object.gshare + ')'
        else:
            gshare = query_object.gshare
        if '*' not in query_object.dst_tier:
            dst_tier = '(' + query_object.dst_tier + ')'
        else:
            dst_tier = query_object.dst_tier
        if '*' not in query_object.processingtype:
            processingtype = '(' + query_object.processingtype + ')'
        else:
            processingtype = query_object.processingtype
        if '*' not in query_object.nucleus:
            nucleus = '(' + query_object.nucleus + ')'
        else:
            nucleus = query_object.nucleus
        if '*' not in query_object.error_category:
            error_category = '(' + query_object.error_category + ')'
        else:
            error_category = query_object.error_category
        if '*' not in query_object.prodsourcelabel:
            prodsourcelabel = '(' + query_object.prodsourcelabel + ')'
        else:
            prodsourcelabel = query_object.prodsourcelabel

        query_base = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["%s*"]}\n""" % (self._get_index(query_object.table))
        query_date = """{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":%s,"lte":%s,"format":"epoch_millis"}}},""" % (startMillisec, endMillisec)
        query_string = """{"query_string":{"analyze_wildcard":true,"query":"data.dst_experiment_site:%s AND data.dst_cloud:%s AND data.dst_country:%s AND data.dst_federation:%s AND data.adcactivity:%s AND data.resourcesreporting:%s AND data.actualcorecount:%s AND data.resource_type:%s AND data.workinggroup:%s AND data.inputfiletype:%s AND data.eventservice:%s AND data.inputfileproject:%s AND data.outputproject:%s AND data.jobstatus:%s AND data.computingsite:%s AND data.gshare:%s AND data.dst_tier:%s AND data.processingtype:%s AND ((NOT _exists_:data.nucleus) OR (data.nucleus:%s)) AND ((NOT _exists_:data.error_category) OR data.error_category:%s) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:%s))"}}]}},""" % \
                       (dst_experiment_site,
                       dst_cloud, dst_country,
                       dst_federation, adcactivity, resourcesreporting, actualcorecount, resource_type, workinggroup,
                       inputfiletype, eventservice, inputfileproject, outputproject, jobstatus, computingsite, gshare,
                       dst_tier, processingtype, nucleus, error_category, prodsourcelabel)

        query_agg_query_time_template = """"aggs":{"time":{"date_histogram":{"interval":"%s","field":"metadata.timestamp","min_doc_count":0,"extended_bounds":{"min":%s,"max":%s},"format":"epoch_millis"},%s}}""" % (query_object.bin, startMillisec, endMillisec, '%s')
        query_agg_func_template = self._get_complete_metrics_function(query_object.field)

        query_agg_template = self._get_complete_agg_function(query_object.grouping, query_agg_query_time_template, query_agg_func_template)
        query = query_base + query_date + query_string + query_agg_template + "\n"

        return query

    def get_data(self, query):
        datasource_url = "api/datasources/proxy/{0}/_msearch?max_concurrent_shard_requests=256".format(self._get_datasource(query.table))
        request_url = "%s/%s" % (self.grafana_proxy, datasource_url)
        request_query = self.get_query(query)
        response = post(request_url, headers=self.grafana_headers, data=request_query)
        print(request_query)
        if response.ok:
            result = loads(response.text)['responses'][0]['aggregations']

        return result

    def _get_complete_agg_function(self, input_groupby, agg_time_template='', agg_func_template=''):
        agg_term_template = """"aggs":{"%s":{"terms":{"field":"data.%s","size":500,"order":{"_key":"desc"},"min_doc_count":1},%s}}"""
        def fill_agg_terms(nested_temp, term, nested_func='%s'):
            nested_temp = nested_temp % (term, term, nested_func)
            return nested_temp

        temp_agg_temp = agg_term_template
        groupby = input_groupby.split(',')
        for option in groupby:
            if option != 'time':
                if option != groupby[-1]:
                    temp_agg_temp = fill_agg_terms(temp_agg_temp, option, agg_term_template)
                else:
                    temp_agg_temp = fill_agg_terms(temp_agg_temp, option, '%s}}')
        if "time" in groupby:
           temp_agg_temp = temp_agg_temp % (agg_time_template)
        temp_agg_temp = temp_agg_temp % (agg_func_template)
        return temp_agg_temp

    def _get_complete_metrics_function(self, input_fields, agg_func='sum'):
        agg_func_template = """"%s":{"%s":{"field":"data.%s","missing":0,"script":{"inline":"_value / 1"}}}"""
        def fill_agg_terms(nested_temp, term, nested_func='%s'):
            nested_temp = nested_temp % (term, agg_func, term, nested_func)
            return nested_temp
        temp_agg_temp = agg_func_template
        quotes = ''
        if ',' in input_fields:
            fields = input_fields.split(',')

            for field in fields:
                if field != fields[-1]:
                    quotes = quotes + '}'
                    temp_agg_temp = temp_agg_temp + ",%s"
                    temp_agg_temp = fill_agg_terms(temp_agg_temp, field, agg_func_template)
                else:
                    quotes = quotes + '}'
                    temp_agg_temp = temp_agg_temp + "%s"
                    temp_agg_temp = fill_agg_terms(temp_agg_temp, field, quotes)
            temp_agg_temp = """"aggs":{""" + temp_agg_temp
        else:
            temp_agg_temp = """"aggs":{"%s":{"%s":{"field":"data.%s","missing":0,"script":{"inline":"_value / 1"}}}"""% (input_fields, agg_func, input_fields)

        return temp_agg_temp