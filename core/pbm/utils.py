"""
    pbm.utils
    
"""
import logging
import pytz
from datetime import datetime, timedelta

from django.db.models import Count, Sum

from .models import DailyLog
from .ADC_colors import ADC_COLOR

_logger = logging.getLogger('bigpandamon-pbm')


defaultDatetimeFormat = '%Y-%m-%d'


CATEGORY_LABELS = {
    'A': 'User selected a site', \
    'B': 'User selected a cloud', \
    'C': 'PanDA decides destination', \
    'D': 'Skip by Panda', \
    'E': 'User excluded a site', \
    'E+': 'With exclude', \
    'E-': 'Without exclude', \

}


PLOT_TITLES = {
    'title01': 'User selected a site/User selected a cloud/PanDA Brokerage decision on Jobs', \
    'title02': 'User selected a site/User selected a cloud/PanDA Brokerage decision on jobDef', \
    'title03': 'User selected a site/User selected a cloud/PanDA Brokerage decision on jobSet', \

    'title04': 'User selected a site on Jobs - Top sites with share > 1 %', \
    'title05': 'User selected a site on jobDef - Top sites with share > 1 %', \
    'title06': 'User selected a site on jobSet - Top sites with share > 1 %', \
    'title07': 'User selected a site on Jobs - Per cloud', \
    'title08': 'User selected a site on JobDef - Per cloud', \
    'title09': 'User selected a site on JobSet - Per cloud', \

    ### plots 10 .. 12 are not used, we don't have data for them, since site==cloud for them
    'title10': 'User selected a cloud on Jobs - Top sites with share > 1 %', \
    'title11': 'User selected a cloud on jobDef - Top sites with share > 1 %', \
    'title12': 'User selected a cloud on jobSet - Top sites with share > 1 %', \
    'title13': 'User selected a cloud on Jobs - Per cloud', \
    'title14': 'User selected a cloud on JobDef - Per cloud', \
    'title15': 'User selected a cloud on JobSet - Per cloud', \

    'title16': 'PanDA Brokerage decision on Jobs - Top sites with share > 1 %', \
    'title17': 'PanDA Brokerage decision on JobDef - Top sites with share > 1 %', \

    'title18': 'PanDA Brokerage decision on Jobs - Per cloud', \
    'title19': 'PanDA Brokerage decision on JobDef - Per cloud', \

    'title20': 'User excluded a site on distinct jobSet - With exclude / Without exclude', \

    'title21': 'User excluded a site on jobSet - Top sites with share > 1 %', \
    'title22': 'User excluded a site on distinct DnUser - Top sites with share > 1 %', \
    'title23': 'User excluded a site on jobSet - Per cloud', \
    'title24': 'User excluded a site on distinct DnUser - Per cloud', \

    'title25': 'Jobs submitted by Country', \
    'title26': 'JobDefs submitted by Country', \
    'title27': 'JobSets submitted by Country', \
}


PLOT_UNITS = {
    '01': 'jobs', \
    '02': 'jobDefs', \
    '03': 'jobSets', \

    '04': 'jobs', \
    '05': 'jobDefs', \
    '06': 'jobSets', \
    '07': 'jobs', \
    '08': 'jobDefs', \
    '09': 'jobSets', \

    ### plots 10 .. 12 are not used, we don't have data for them, since site==cloud for them
    '10': 'jobs', \
    '11': 'jobDefs', \
    '12': 'jobSets', \

    '13': 'jobs', \
    '14': 'jobDefs', \
    '15': 'jobSets', \

    '16': 'jobs', \
    '17': 'jobDefs', \

    '18': 'jobs', \
    '19': 'jobDefs', \

    '20': 'jobSets', \

    '21': 'jobSets', \
    '22': 'UserDNs', \
    '23': 'jobSets', \
    '24': 'UserDNs', \

    '25': 'jobs', \
    '26': 'JobDefs', \
    '27': 'JobSets', \
}


COLORS = {
    '01': ['#FF0000', '#50B432', '#0000FF'],
    '02': ['#FF0000', '#50B432', '#0000FF'],
    '03': ['#FF0000', '#50B432', '#0000FF'],
    '20': ['#FF0000', '#0000FF'],
    '25': ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],
    '26': ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],
    '27': ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],

}


def get_colors_dictionary(data, cutoff=None):
    colors = {}
    counter = {}
    ### init cloud item counters
    for cloud in ADC_COLOR.keys():
        counter[cloud] = 0
    ### loop over data, increment cloud counters -> get predefined colors for sites/clouds
    for item in data:
        append = True
        if cutoff is not None:
            if cutoff < float(item['percent'][:-1]):
                append = True
            else:
                append = False
        if append:
            try:
                cloud = item['cloud']
                if cloud in counter:
                    item_color = ADC_COLOR[cloud][counter[cloud]]
                    if 'site' in item:
                        counter[cloud] += 1
                    if 'country' in item:
                        counter[cloud] += 1
                else:
                    item_color = '#FFFFFF'
            except:
                item_color = '#FFFFFF'
            if 'site' in item:
                colors[item['site']] = item_color
            else:
                colors[item['cloud']] = item_color
    return colors


def prepare_data_for_piechart(data, unit='jobs', cutoff=None):
    """
        prepare_data_for_piechart
        
        
        data ... result of a queryset
        unit ... 'jobs', or 'jobDefs', or 'jobSets'
        cutoff ... anything with share smaller than cutoff percent will be grouped into 'Other' 
        
        example input:
            data = [{'category': u'A', 'sum': 13046, 'percent': '7.90%', 'label': 'User selected a site'}, 
                    {'category': u'B', 'sum': 157, 'percent': '0.10%', 'label': 'User selected a cloud'}, 
                    {'category': u'C', 'sum': 151990, 'percent': '92.01%', 'label': 'PanDA decides destination'}
            ] 
        example output:
            piechart_data = [ ['User selected a site', 13046], 
                              ['User selected a cloud', 157], 
                              ['PanDA decides destination', 151990] 
            ]
    """
    piechart_data = []
    other_item_sum = 0
    for item in data:
        append = True
        if cutoff is not None:
            if cutoff < float(item['percent'][:-1]):
                append = True
            else:
                append = False
                other_item_sum += int(item['sum'])
        if append:
            piechart_data.append([ str('%s (%s %s)' % (item['label'], item['sum'], unit)), item['sum']])
    if other_item_sum > 0:
        piechart_data.append(['Other (%s %s)' % (other_item_sum, unit), other_item_sum])
    return piechart_data


def prepare_colors_for_piechart(data, cutoff=None):
    """
        prepare_colors_for_piechart
        
        
        data ... result of a queryset
        unit ... 'jobs', or 'jobDefs', or 'jobSets'
        cutoff ... anything with share smaller than cutoff percent will be grouped into 'Other' 
        
    """
    colors_names = get_colors_dictionary(data, cutoff)
    colors = []
    other_item_sum = 0
    for item in data:
        append = True
        if cutoff is not None:
            if cutoff < float(item['percent'][:-1]):
                append = True
            else:
                append = False
                other_item_sum += int(item['sum'])
        if append:
            if item['name'] in colors_names:
                colors.append(colors_names[item['name']])
            else:
                colors.append('#CCCCCC')
    if other_item_sum > 0:
        colors.append('#CCCCCC')
    return colors


def configure(request_GET):
    errors_GET = {}
    ### if startdate&enddate are provided, use them
    if 'startdate' in request_GET and 'enddate' in request_GET:
        ndays = -1
        ### startdate
        startdate = request_GET['startdate']
        try:
            dt_start = datetime.strptime(startdate, defaultDatetimeFormat)
        except ValueError:
            errors_GET['startdate'] = \
                'Provided startdate [%s] has incorrect format, expected [%s].' % \
                (startdate, defaultDatetimeFormat)
            startdate = datetime.utcnow() - timedelta(days=ndays)
            startdate = startdate.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        ### enddate
        enddate = request_GET['enddate']
        try:
            dt_end = datetime.strptime(enddate, defaultDatetimeFormat)
        except ValueError:
            errors_GET['enddate'] = \
                'Provided enddate [%s] has incorrect format, expected [%s].' % \
                (enddate, defaultDatetimeFormat)
            enddate = datetime.utcnow()
            enddate = enddate.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    ### if ndays is provided, do query "last N days"
    elif 'ndays' in request_GET:
        try:
            ndays = int(request_GET['ndays'])
        except:
            ndays = 8
            errors_GET['ndays'] = \
                'Wrong or no ndays has been provided.Using [%s].' % \
                (ndays)
        startdate = datetime.utcnow() - timedelta(days=ndays)
        startdate = startdate.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        enddate = datetime.utcnow()
        enddate = enddate.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
    ### neither ndays, nor startdate&enddate was provided
    else:
        ndays = 8
        startdate = datetime.utcnow() - timedelta(days=ndays)
        startdate = startdate.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        enddate = datetime.utcnow()
        enddate = enddate.replace(tzinfo=pytz.utc).strftime(defaultDatetimeFormat)
        errors_GET['noparams'] = \
                'Neither ndays, nor startdate & enddate has been provided. Using startdate=%s and enddate=%s.' % \
                (startdate, enddate)
    
    return startdate, enddate, ndays, errors_GET


def configure_plot(request_GET):
    ### if plotid is provided, use it
    if 'plotid' in request_GET:
        plotid = request_GET['plotid']
    ### plotid was not provided
    else:
        plotid = 0
    return plotid


def data_plot_groupby_category(query, values=['category'], \
        sum_param='jobcount', label_cols=['category'], label_translation=True, \
        order_by=[]):
    pre_data_01 = DailyLog.objects.filter(**query).values(*values).annotate(sum=Sum(sum_param))
    if len(order_by):
        pre_data_01 = pre_data_01.order_by(*order_by)
    total_data_01 = sum([x['sum'] for x in pre_data_01])
    data01 = []
    colors01 = {}
    for item in pre_data_01:
        item['name'] = item[label_cols[0]]
        item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_01)
        if label_translation:
            if len(label_cols) > 1:
                item['label'] = '%s (%s)' % (item[label_cols[0]], item[label_cols[1]])
            else:
                item['label'] = CATEGORY_LABELS[ item[label_cols[0]] ]
        else:
            if len(label_cols) > 1:
                item['label'] = '%s (%s)' % (item[label_cols[0]], item[label_cols[1]])
            else:
                item['label'] = item[label_cols[0]]
        data01.append(item)
    return data01


def plot_nothing(id, query):
    return [], [], PLOT_TITLES['title' + id]


def plot_01(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C']
    data = data_plot_groupby_category(query, values=['category'], sum_param='jobcount', \
                    label_cols=['category'], label_translation=True)
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_02(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C']
    data = data_plot_groupby_category(query, values=['category'], sum_param='jobdefcount', \
                    label_cols=['category'], label_translation=True)
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_03(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C']
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_03 = DailyLog.objects.filter(**query).distinct('jobset').values('category').annotate(sum=Count('jobset'))
        total_data_03 = sum([x['sum'] for x in pre_data_03])
        for item in pre_data_03:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_03)
            item['label'] = CATEGORY_LABELS[ item['category'] ]
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_03 = DailyLog.objects.filter(**query).values('category', 'jobset')
        categories = list(set([ x['category'] for x in pre_data_03]))
        pre2_data_03 = []
        total_data_03 = 0
        for category in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_03 if x['category'] == category]))
            pre2_data_03.append({'category': category, 'sum': len(jobsets_for_category)})
            total_data_03 += len(jobsets_for_category)
        for item in pre2_data_03:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_03)
            item['label'] = CATEGORY_LABELS[ item['category'] ]
            data.append(item)
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_04(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'A'
    data = data_plot_groupby_category(query, values=['category', 'site', 'cloud'], sum_param='jobcount', \
                    label_cols=['site', 'cloud'], label_translation=False, \
                    order_by=['cloud', 'site'])
    colors = prepare_colors_for_piechart(data, cutoff=1.0)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_05(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'A'
    data = data_plot_groupby_category(query, values=['category', 'site', 'cloud'], sum_param='jobdefcount', \
                    label_cols=['site', 'cloud'], label_translation=False, \
                    order_by=['cloud', 'site'])
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_06(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'A'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_06 = DailyLog.objects.filter(**query).distinct('jobset').values('site', 'cloud').annotate(sum=Count('jobset')).order_by('cloud', 'site')
        total_data_06 = sum([x['sum'] for x in pre_data_06])
        for item in pre_data_06:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_06)
            item['label'] = '%s (%s)' % (item['site'], item['cloud'])
            item['name'] = item['site']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_06 = DailyLog.objects.filter(**query).values('site', 'cloud', 'jobset').order_by('cloud', 'site')
        categories = list(set([ (x['site'], x['cloud']) for x in pre_data_06]))
        pre2_data_06 = []
        total_data_06 = 0
        for category, cat2 in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_06 if x['site'] == category]))
            pre2_data_06.append({'site': category, 'cloud': cat2, 'sum': len(jobsets_for_category)})
            total_data_06 += len(jobsets_for_category)
        for item in pre2_data_06:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_06)
            item['label'] = '%s (%s)' % (item['site'], item['cloud'])
            item['name'] = item['site']
            data.append(item)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_07(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'A'
    data = data_plot_groupby_category(query, values=['category', 'cloud'], sum_param='jobcount', \
                    label_cols=['cloud'], label_translation=False)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_08(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'A'
    data = data_plot_groupby_category(query, values=['category', 'cloud'], sum_param='jobdefcount', \
                    label_cols=['cloud'], label_translation=False)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_09(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'A'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_09 = DailyLog.objects.filter(**query).distinct('jobset').values('cloud').annotate(sum=Count('jobset'))
        total_data_09 = sum([x['sum'] for x in pre_data_09])
        for item in pre_data_09:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_09)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_09 = DailyLog.objects.filter(**query).values('cloud', 'jobset')
        categories = list(set([ x['cloud'] for x in pre_data_09]))
        pre2_data_09 = []
        total_data_09 = 0
        for category in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_09 if x['cloud'] == category]))
            pre2_data_09.append({'cloud': category, 'sum': len(jobsets_for_category)})
            total_data_09 += len(jobsets_for_category)
        for item in pre2_data_09:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_09)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_13(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'B'
    data = data_plot_groupby_category(query, values=['category', 'cloud'], sum_param='jobcount', \
                    label_cols=['cloud'], label_translation=False)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_14(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'B'
    data = data_plot_groupby_category(query, values=['category', 'cloud'], sum_param='jobdefcount', \
                    label_cols=['cloud'], label_translation=False)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_15(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'B'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_15 = DailyLog.objects.filter(**query).distinct('jobset').values('cloud').annotate(sum=Count('jobset'))
        total_data_15 = sum([x['sum'] for x in pre_data_15])
        for item in pre_data_15:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_15)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_15 = DailyLog.objects.filter(**query).values('cloud', 'jobset')
        categories = list(set([ x['cloud'] for x in pre_data_15]))
        pre2_data_15 = []
        total_data_15 = 0
        for category in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_15 if x['cloud'] == category]))
            pre2_data_15.append({'cloud': category, 'sum': len(jobsets_for_category)})
            total_data_15 += len(jobsets_for_category)
        for item in pre2_data_15:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_15)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_16(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'C'
    data = data_plot_groupby_category(query, values=['category', 'site', 'cloud'], sum_param='jobcount', \
                    label_cols=['site', 'cloud'], label_translation=False, \
                    order_by=['cloud', 'site'])
    colors = prepare_colors_for_piechart(data, cutoff=1.0)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_17(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'C'
    data = data_plot_groupby_category(query, values=['category', 'site', 'cloud'], sum_param='jobdefcount', \
                    label_cols=['site', 'cloud'], label_translation=False, \
                    order_by=['cloud', 'site'])
    colors = prepare_colors_for_piechart(data, cutoff=1.0)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_18(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'C'
    data = data_plot_groupby_category(query, values=['category', 'cloud'], sum_param='jobcount', \
                    label_cols=['cloud'], label_translation=False)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_19(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'C'
    data = data_plot_groupby_category(query, values=['category', 'cloud'], sum_param='jobdefcount', \
                    label_cols=['cloud'], label_translation=False)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_20(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C', 'E']
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_20 = DailyLog.objects.filter(**query).distinct('jobset').values('category').annotate(sum=Count('jobset'))
        total_data_20 = sum([x['sum'] for x in pre_data_20])
        pre2_data_20 = []
        for item in [x for x in pre_data_20 if x['category'] == 'E']:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_20)
            item['label'] = CATEGORY_LABELS[ 'E+' ]
            data.append(item)
        not_excluded = [x for x in pre_data_20 if x['category'] != 'E']
        for item in not_excluded[:1]:
            item['percent'] = '%.2f%%' % (100.0 * sum([x['sum'] for x in not_excluded]) / total_data_20)
            item['label'] = CATEGORY_LABELS[ 'E-' ]
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_20 = DailyLog.objects.filter(**query).values('category', 'jobset')
        excluded = list(set([ x['jobset'] for x in pre_data_20 if x['category'] == 'E']))
        not_excluded = list(set([ x['jobset'] for x in pre_data_20 if x['category'] != 'E']))
        if len(excluded) + len(not_excluded) > 0:
            data.append({'category': 'E', 'sum': len(excluded), \
                           'percent': '%.2f%%' % (100.0 * len(excluded) / (len(excluded) + len(not_excluded))), \
                           'label': CATEGORY_LABELS[ 'E+' ]\
                           })
            data.append({'category': 'ABC', 'sum': len(not_excluded), \
                           'percent': '%.2f%%' % (100.0 * len(not_excluded) / (len(excluded) + len(not_excluded))), \
                           'label': CATEGORY_LABELS[ 'E-' ]\
                           })
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_21(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'E'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_21 = DailyLog.objects.filter(**query).distinct('jobset').values('site', 'cloud').annotate(sum=Count('jobset')).order_by('cloud', 'site')
        total_data_21 = sum([x['sum'] for x in pre_data_21])
        for item in pre_data_21:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_21)
            item['label'] = '%s (%s)' % (item['site'], item['cloud'])
            item['name'] = item['site']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_21 = DailyLog.objects.filter(**query).values('site', 'cloud', 'jobset')
        categories = list(set([ (x['site'], x['cloud']) for x in pre_data_21]))
        pre2_data_21 = []
        total_data_21 = 0
        for category, cat2 in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_21 if x['site'] == category]))
            pre2_data_21.append({'site': category, 'cloud': cat2, 'sum': len(jobsets_for_category)})
            total_data_21 += len(jobsets_for_category)
        for item in pre2_data_21:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_21)
            item['label'] = '%s (%s)' % (item['site'], item['cloud'])
            item['name'] = item['site']
            data.append(item)
    colors = prepare_colors_for_piechart(data, cutoff=1.0)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_22(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'E'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_22 = DailyLog.objects.filter(**query).distinct('dnuser').values('site', 'cloud').annotate(sum=Count('dnuser')).order_by('cloud', 'site')
        total_data_22 = sum([x['sum'] for x in pre_data_22])
        for item in pre_data_22:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_22)
            item['label'] = '%s (%s)' % (item['site'], item['cloud'])
            item['name'] = item['site']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_22 = DailyLog.objects.filter(**query).values('site', 'cloud', 'dnuser')
        categories = list(set([ (x['site'], x['cloud']) for x in pre_data_22]))
        pre2_data_22 = []
        total_data_22 = 0
        for category, cat2 in sorted(categories):
            dnusers_for_category = list(set([x['dnuser'] for x in pre_data_22 if x['site'] == category]))
            pre2_data_22.append({'site': category, 'cloud': cat2, 'sum': len(dnusers_for_category)})
            total_data_22 += len(dnusers_for_category)
        for item in pre2_data_22:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_22)
            item['label'] = '%s (%s)' % (item['site'], item['cloud'])
            item['name'] = item['site']
            data.append(item)
    colors = prepare_colors_for_piechart(data, cutoff=1.0)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_23(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'E'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_23 = DailyLog.objects.filter(**query).distinct('jobset').values('cloud').annotate(sum=Count('jobset')).order_by('cloud')
        total_data_23 = sum([x['sum'] for x in pre_data_23])
        for item in pre_data_23:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_23)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_23 = DailyLog.objects.filter(**query).values('cloud', 'jobset')
        categories = list(set([ x['cloud'] for x in pre_data_23]))
        pre2_data_23 = []
        total_data_23 = 0
        for category in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_23 if x['cloud'] == category]))
            pre2_data_23.append({'cloud': category, 'sum': len(jobsets_for_category)})
            total_data_23 += len(jobsets_for_category)
        for item in pre2_data_23:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_23)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_24(id, query):
    if 'category__in' in query:
        del query['category__in']
    query['category'] = 'E'
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_24 = DailyLog.objects.filter(**query).distinct('dnuser').values('cloud').annotate(sum=Count('dnuser')).order_by('cloud')
        total_data_24 = sum([x['sum'] for x in pre_data_24])
        for item in pre_data_24:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_24)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_24 = DailyLog.objects.filter(**query).values('site', 'cloud', 'dnuser')
        categories = list(set([ x['cloud'] for x in pre_data_24]))
        pre2_data_24 = []
        total_data_24 = 0
        for category in sorted(categories):
            dnusers_for_category = list(set([x['dnuser'] for x in pre_data_24 if x['cloud'] == category]))
            pre2_data_24.append({'cloud': category, 'sum': len(dnusers_for_category)})
            total_data_24 += len(dnusers_for_category)
        for item in pre2_data_24:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_24)
            item['label'] = item['cloud']
            item['name'] = item['cloud']
            data.append(item)
    colors = prepare_colors_for_piechart(data)
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_25(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C', 'E']
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_25 = DailyLog.objects.filter(**query).distinct('country').values('country', 'jobcount').annotate(sum=Sum('jobcount')).order_by('country')
        total_data_25 = sum([x['sum'] for x in pre_data_25])
        for item in pre_data_25:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_25)
            item['label'] = item['country']
            item['name'] = item['country']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_25 = DailyLog.objects.filter(**query).values('country', 'jobcount')
        categories = list(set([ x['country'] for x in pre_data_25]))
        pre2_data_25 = []
        total_data_25 = 0
        for category in sorted(categories):
            jobsets_for_category = [x['jobcount'] for x in pre_data_25 if x['country'] == category]
            pre2_data_25.append({'country': category, 'sum': sum(jobsets_for_category)})
            total_data_25 += len(jobsets_for_category)
        for item in pre2_data_25:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_25)
            item['label'] = item['country']
            item['name'] = item['country']
            data.append(item)
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_26(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C', 'E']
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_26 = DailyLog.objects.filter(**query).distinct('country').values('country', 'jobdefcount').annotate(sum=Sum('jobdefcount')).order_by('country')
        total_data_26 = sum([x['sum'] for x in pre_data_26])
        for item in pre_data_26:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_26)
            item['label'] = item['country']
            item['name'] = item['country']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_26 = DailyLog.objects.filter(**query).values('country', 'jobdefcount')
        categories = list(set([ x['country'] for x in pre_data_26]))
        pre2_data_26 = []
        total_data_26 = 0
        for category in sorted(categories):
            jobsets_for_category = [x['jobdefcount'] for x in pre_data_26 if x['country'] == category]
            pre2_data_26.append({'country': category, 'sum': sum(jobsets_for_category)})
            total_data_26 += len(jobsets_for_category)
        for item in pre2_data_26:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_26)
            item['label'] = item['country']
            item['name'] = item['country']
            data.append(item)
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot_27(id, query):
    if 'category' in query:
        del query['category']
    query['category__in'] = ['A', 'B', 'C', 'E']
    data = []
    try:
        ### TODO: FIXME: check that this pre_data_03 queryset works on MySQL and Oracle
        pre_data_27 = DailyLog.objects.filter(**query).distinct('jobset').values('country').annotate(sum=Count('jobset'))
        total_data_27 = sum([x['sum'] for x in pre_data_27])
        for item in pre_data_27:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_27)
            item['label'] = item['country']
            item['name'] = item['country']
            data.append(item)
    except NotImplementedError:
        ### This is queryset and aggregation for SQLite3 backend, as .distinct('jobset') raises NotImplementedError on SQLite3
        pre_data_27 = DailyLog.objects.filter(**query).values('country', 'jobset')
        categories = list(set([ x['country'] for x in pre_data_27]))
        pre2_data_27 = []
        total_data_27 = 0
        for category in sorted(categories):
            jobsets_for_category = list(set([x['jobset'] for x in pre_data_27 if x['country'] == category]))
            pre2_data_27.append({'country': category, 'sum': len(jobsets_for_category)})
            total_data_27 += len(jobsets_for_category)
        for item in pre2_data_27:
            item['percent'] = '%.2f%%' % (100.0 * item['sum'] / total_data_27)
            item['label'] = item['country']
            item['name'] = item['country']
            data.append(item)
    colors = COLORS[id]
    title = PLOT_TITLES['title' + id]
    unit = PLOT_UNITS[id]
    return data, colors, title, unit


def plot(id, query):
    dispatch = {
        '0': plot_nothing, \
        '01': plot_01, \
        '02': plot_02, \
        '03': plot_03, \
        '04': plot_04, \
        '05': plot_05, \
        '06': plot_06, \
        '07': plot_07, \
        '08': plot_08, \
        '09': plot_09, \
        '10': plot_nothing, \
        '11': plot_nothing, \
        '12': plot_nothing, \
        '13': plot_13, \
        '14': plot_14, \
        '15': plot_15, \
        '16': plot_16, \
        '17': plot_17, \
        '18': plot_18, \
        '19': plot_19, \
        '20': plot_20, \
        '21': plot_21, \
        '22': plot_22, \
        '23': plot_23, \
        '24': plot_24, \
        '25': plot_25, \
        '26': plot_26, \
        '27': plot_27, \
    }
    if id not in dispatch:
        return dispatch[0](id, query)
    return dispatch[id](id, query)


