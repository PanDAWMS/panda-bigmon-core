"""

"""
import re

from django.db.models import Case, When, Value, Sum
from core.globalshares.models import JobsShareStats
from decimal import Decimal, InvalidOperation

from core.globalshares import GlobalShares
from core.globalshares.models import GlobalSharesModel

from core.schedresource.utils import get_pq_resource_types


def get_child_elements(tree,childsgsharelist):
    for gshare in tree:
        if gshare!='childlist':
            shortGshare = re.sub(r'\[(.*)\]', '', gshare).rstrip()
            if 'childlist' in tree[gshare] and len(tree[gshare]['childlist'])==0:#len(tree[gshare])==0:
                childsgsharelist.append(shortGshare)
            elif 'childlist' not in tree[gshare]:
                childsgsharelist.append(shortGshare)
            else:
                get_child_elements(tree[gshare],childsgsharelist)


def get_child_sumstats(childsgsharelist,resourcesdict,gshare):
    parentgshare = {}
    parentgshare[gshare] = {}
    for child in childsgsharelist:
        if child in resourcesdict:
            for resource in resourcesdict[child]:
                if resource not in parentgshare[gshare]:
                    parentgshare[gshare][resource] = {}
                    for k in resourcesdict[child][resource].keys():
                        parentgshare[gshare][resource][k] = resourcesdict[child][resource][k]
                else:
                    for k in resourcesdict[child][resource].keys():
                            parentgshare[gshare][resource][k] += resourcesdict[child][resource][k]
    return parentgshare


def get_hs_distribution(group_by='gshare', out_format='dict'):
    """
    Get HS06s aggregation from jobs_share_stats table
    :param group_by: field to group by
    :return:
    """
    group_by_list = []
    if type(group_by) in (list, tuple) and len(set(group_by) - set([f.name for f in JobsShareStats._meta.get_fields()])) == 0:
        group_by_list = list(group_by)
    elif isinstance(group_by, str) and group_by in [f.name for f in JobsShareStats._meta.get_fields()]:
        group_by_list.append(group_by)
    else:
        return []
    group_by_list.append('jobstatus_grouped')
    hs_distribution = JobsShareStats.objects.annotate(
        jobstatus_grouped=Case(
            When(jobstatus='activated', then=Value('queued')),
            When(jobstatus__in=('sent', 'running'), then=Value('executing')),
            default=Value('ignore')
        )
    ).values(*group_by_list).annotate(hs_sum=Sum('hs'))
    group_by_list.append('hs_sum')
    if out_format == 'tuple':
        hs_distribution = [tuple(row[v] for v in group_by_list) for row in hs_distribution]

    return hs_distribution

def get_gs_plots_data(gs_list, resources_dict, gs_tree_dict):
    gs_plot_data = {
        'level1': {
            'pieChartActualHS06': {},
            'barChartActualVSTarget': {'resourceTypeList': [], 'data': {}}
        },
        'level2': {
            'pieChartActualHS06': {},
            'barChartActualVSTarget': {'resourceTypeList': [], 'data': {}}
        },
        'level3': {
            'pieChartActualHS06': {},
            'barChartActualVSTarget': {'resourceTypeList': [], 'data': {}}
        },
    }

    for l1 in sorted(gs_tree_dict['childlist']):
        for gs in gs_list:
            if l1 in gs['level1']:
                gs_name = re.sub(r'\[(.*)\]', '', l1).rstrip()
                level = 'level1'
                gs_tree_dict_level = gs_tree_dict[l1]
                gs_plot_data = fill_level_gs_plots_data(gs_plot_data, level, gs_name, gs, resources_dict,
                                                        gs_tree_dict_level)

                if 'childlist' in gs_tree_dict_level and len(gs_tree_dict_level['childlist']) == 0:
                    gs_plot_data = fill_level_gs_plots_data(gs_plot_data, 'level2', gs_name, gs, resources_dict,
                                                            gs_tree_dict_level)
                    gs_plot_data = fill_level_gs_plots_data(gs_plot_data, 'level3', gs_name, gs, resources_dict,
                                                            gs_tree_dict_level)

        for l2 in sorted(gs_tree_dict[l1]['childlist']):
            for gs in gs_list:
                if l2 in gs['level2']:
                    gs_name = re.sub(r'\[(.*)\]', '', l2).rstrip()
                    level = 'level2'
                    gs_tree_dict_level = gs_tree_dict[l1][l2]
                    gs_plot_data = fill_level_gs_plots_data(gs_plot_data, level, gs_name, gs, resources_dict,
                                                            gs_tree_dict_level)

                    if 'childlist' in gs_tree_dict_level and len(gs_tree_dict_level['childlist']) == 0:
                        gs_plot_data = fill_level_gs_plots_data(gs_plot_data, 'level3', gs_name, gs, resources_dict,
                                                                gs_tree_dict_level)

            for l3 in sorted(gs_tree_dict[l1][l2]['childlist']):
                for gs in gs_list:
                    if l3 in gs['level3']:
                        gs_name = re.sub(r'\[(.*)\]', '', l3).rstrip()
                        level = 'level3'
                        gs_tree_dict_level = gs_tree_dict[l1][l2][l3]
                        gs_plot_data = fill_level_gs_plots_data(gs_plot_data, level, gs_name, gs, resources_dict,
                                                                gs_tree_dict_level)

    #
    for level, plots in gs_plot_data.items():
        for plot, plot_dict in plots.items():
            if plot.startswith('pie'):
                gs_plot_data[level][plot] = [[gs, int(value)] for gs, value in plot_dict.items() if value > 0]
                gs_plot_data[level][plot] = sorted(gs_plot_data[level][plot], key=lambda x: x[0])
            elif plot.startswith('bar'):
                temp_dict = {}
                for gs, gs_dict in plot_dict['data'].items():
                    if len(gs_dict['Actual']) > 0 and sum(gs_dict['Actual'].values()) > 0 and gs not in temp_dict:
                        temp_dict[gs] = gs_dict
                        gs_plot_data[level][plot]['resourceTypeList'].extend(gs_dict['Actual'].keys())
                gs_plot_data[level][plot]['data'] = temp_dict
                gs_plot_data[level][plot]['resourceTypeList'] = list(set(gs_plot_data[level][plot]['resourceTypeList']))

    return gs_plot_data


def fill_level_gs_plots_data(gs_plot_data, level, gs_name, gs, resources_dict, gs_tree_dict_level):
    if gs_name not in gs_plot_data[level]['barChartActualVSTarget']['data']:
        gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name] = {
            'Actual': {},
            'Target': float(gs['pledged']),
        }
    if gs_name not in gs_plot_data[level]['pieChartActualHS06']:
        gs_plot_data[level]['pieChartActualHS06'][gs_name] = 0
    if 'childlist' not in gs_tree_dict_level or len(gs_tree_dict_level['childlist']) == 0:
        if gs_name in resources_dict:
            for r_name, r_dict in resources_dict[gs_name].items():
                if r_name not in gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name]['Actual']:
                    gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name]['Actual'][r_name] = 0
                gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name]['Actual'][r_name] += float(r_dict['executing'])
                gs_plot_data[level]['pieChartActualHS06'][gs_name] += float(r_dict['executing'])
    else:
        gs_child_list = []
        get_child_elements(gs_tree_dict_level, gs_child_list)
        gs_resources_dict = get_child_sumstats(gs_child_list, resources_dict, gs_name)
        for r_name, r_dict in gs_resources_dict[gs_name].items():
            if r_name not in gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name]['Actual']:
                gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name]['Actual'][r_name] = 0
            gs_plot_data[level]['barChartActualVSTarget']['data'][gs_name]['Actual'][r_name] += float(r_dict['executing'])
            gs_plot_data[level]['pieChartActualHS06'][gs_name] += float(r_dict['executing'])

    return gs_plot_data
def _safe_percent(ratio, value):
    try:
        if ratio in (None, '', 'NaN') or value in (None, '', 'NaN'):
            return None
        r = Decimal(str(ratio))
        v = Decimal(str(value))
        return (r * v) / Decimal(100)
    except (InvalidOperation, TypeError, ValueError):
        return None


def get_resources_gshare():
    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'
    resourcesDictSites = get_pq_resource_types()
    hs_distribution_raw = get_hs_distribution(group_by=('gshare', 'computingsite'), out_format='tuple')
    # get the hs distribution data into a dictionary structure
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    total_hs = 0
    newresourecurcetype = ''
    resourcecnt = 0
    for hs_entry in hs_distribution_raw:
        gshare, computingsite, status_group, hs = hs_entry
        try:
            resourcetype = resourcesDictSites[computingsite]
        except:
            continue
        hs_distribution_dict.setdefault(gshare,{})
        hs_distribution_dict[gshare].setdefault(resourcetype, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0, IGNORE:0})
        #hs_distribution_dict[gshare][resourcetype][status_group] = hs

        total_hs += hs

        if status_group == QUEUED:
            hs_queued_total += hs
            hs_distribution_dict[gshare][resourcetype][status_group] += hs
        elif status_group == EXECUTING:
            hs_executing_total += hs
            hs_distribution_dict[gshare][resourcetype][status_group] += hs
        else:
            hs_ignore_total += hs
            hs_distribution_dict[gshare][resourcetype][status_group] += hs

    hs_distribution_list=resourcesDictToList(hs_distribution_dict)

    return hs_distribution_list, hs_distribution_dict


def resourcesDictToList(hs_distribution_dict):
    ignore = 0
    pled = 0
    executing = 0
    queued = 0
    total_hs = 0
    for gshare in hs_distribution_dict.keys():
        for resource in hs_distribution_dict[gshare].keys():
            sum_hs = 0
            pled += hs_distribution_dict[gshare][resource]['pledged']
            ignore += hs_distribution_dict[gshare][resource]['ignore']
            executing += hs_distribution_dict[gshare][resource]['executing']
            queued += hs_distribution_dict[gshare][resource]['queued']
            sum_hs = float(hs_distribution_dict[gshare][resource]['pledged']) + \
                 float(hs_distribution_dict[gshare][resource]['ignore']) + \
                 float(hs_distribution_dict[gshare][resource]['executing']) + \
                 float(hs_distribution_dict[gshare][resource]['queued'])
            total_hs+=sum_hs
            hs_distribution_dict[gshare][resource]['total_hs'] = sum_hs

    hs_distribution_list = {}
    for gshare in hs_distribution_dict.keys():
        for resource in hs_distribution_dict[gshare].keys():
            if ignore > 0:
            	hs_distribution_dict[gshare][resource]['ignore_percent'] =  (hs_distribution_dict[gshare][resource]['ignore']/ignore)* 100
            else:
                hs_distribution_dict[gshare][resource]['ignore_percent'] = 0
            if executing > 0:
            	hs_distribution_dict[gshare][resource]['executing_percent'] =  (hs_distribution_dict[gshare][resource]['executing'] / executing) * 100
            else:
                hs_distribution_dict[gshare][resource]['executing_percent'] = 0
            if queued > 0:
            	hs_distribution_dict[gshare][resource]['queued_percent'] = (hs_distribution_dict[gshare][resource]['queued']/queued) * 100
            else:
                hs_distribution_dict[gshare][resource]['queued_percent'] = 0
            hs_distribution_list.setdefault(str(gshare).lower(),[]).append({'resource':resource, 'pledged':hs_distribution_dict[gshare][resource]['pledged'],
                                     'ignore':hs_distribution_dict[gshare][resource]['ignore'],
                                     'ignore_percent':hs_distribution_dict[gshare][resource]['ignore_percent'],
                                     'executing':hs_distribution_dict[gshare][resource]['executing'],
                                     'executing_percent': hs_distribution_dict[gshare][resource]['executing_percent'],
                                     'queued':hs_distribution_dict[gshare][resource]['queued'],
                                     'queued_percent':hs_distribution_dict[gshare][resource]['queued_percent'],
                                     'total_hs':hs_distribution_dict[gshare][resource]['total_hs'],
                                     'total_hs_percent': (hs_distribution_dict[gshare][resource]['total_hs']/total_hs)*100
                                     })
    return hs_distribution_list


def add_resources(gshare,tableRows,resourceslist,level):
    gshare = str(gshare).replace('_', ' ')
    if gshare in resourceslist:
        resourcesForGshare = resourceslist[gshare]
        resourcesForGshareList = []
        if level == 'level1':
            for resource in resourcesForGshare:
                resource['level1'] = resource['resource']
                resource['level2'] = ''
                resource['level3'] = ''
        if level == 'level2':
            for resource in resourcesForGshare:
                resource['level1'] = ''
                resource['level2'] = resource['resource']
                resource['level3'] = ''
        if level == 'level3':
            for resource in resourcesForGshare:
                resource['level1'] = ''
                resource['level2'] = ''
                resource['level3'] = resource['resource']

        for row in tableRows:
            if 'gshare' in row and gshare.replace(' ', '_') == row['gshare']:
                row['resources'] = resourcesForGshare


def get_shares(parents=''):
    """
    Get global shares from DB
    :param parents:
    :return:
    """
    gvalues = ('name', 'value', 'parent', 'prodsourcelabel', 'workinggroup', 'campaign', 'processingtype')
    gquery = {}
    if parents is None:
        gquery['parent__isnull'] = True
    elif type(parents) == str:
        gquery['parent'] = parents
    elif type(parents) in (list, tuple):
        gquery['parent__in'] = parents

    global_shares_list = []
    global_shares_list.extend(GlobalSharesModel.objects.filter(**gquery).values(*gvalues))
    global_shares_tuples = [(tuple(gs[gv] for gv in gvalues)) for gs in global_shares_list]

    return global_shares_tuples


def __load_branch(share):
    """
    Recursively load a branch
    """
    node = GlobalShares.Share(share.name, share.value, share.parent, share.prodsourcelabel,
                              share.workinggroup, share.campaign, share.processingtype)

    children = get_shares(parents=share.name)
    if not children:
        return node

    for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in children:
        child = GlobalShares.Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
        node.children.append(__load_branch(child))

    return node


def __get_hs_leave_distribution():
    """
    Get the current HS06 distribution for running and queued jobs
    """

    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'

    comment = ' /* DBProxy.get_hs_leave_distribution */'

    tree = GlobalShares.Share('root', 100, None, None, None, None, None)
    shares_top_level = get_shares(parents=None)
    for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in shares_top_level:
        share = GlobalShares.Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
        tree.children.append(__load_branch(share))

    tree.normalize()
    leave_shares = tree.get_leaves()

    hs_distribution_raw = get_hs_distribution(group_by='gshare', out_format='tuple')

    # get the hs distribution data into a dictionary structure
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    for hs_entry in hs_distribution_raw:
        gshare, status_group, hs = hs_entry
        hs_distribution_dict.setdefault(gshare, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0})
        hs_distribution_dict[gshare][status_group] = hs
        # calculate totals
        if status_group == QUEUED:
            hs_queued_total += hs
        elif status_group == EXECUTING:
            hs_executing_total += hs
        else:
            hs_ignore_total += hs

    # Calculate the ideal HS06 distribution based on shares.

    for share_node in leave_shares:
        share_name, share_value = share_node.name, share_node.value
        hs_pledged_share = hs_executing_total * Decimal(str(share_value)) / Decimal(str(100.0))

        hs_distribution_dict.setdefault(share_name, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0})
        # Pledged HS according to global share definitions
        hs_distribution_dict[share_name]['pledged'] = hs_pledged_share

    getChildStat(tree, hs_distribution_dict, 0)
    rows = []
    stripTree(tree, rows)
    return hs_distribution_dict, rows


def stripTree(node, rows):
    row = {}
    if hasattr(node,'level'):
        if node.level > 0:
            if node.level == 1:
                row['level1'] = node.name + ' [' + ("%0.1f" % node.rawvalue) + '%]'
                row['level2'] = ''
                row['level3'] = ''
            if node.level == 2:
                row['level1'] = ''
                row['level2'] = node.name + ' [' + ("%0.1f" % node.rawvalue) + '%]'
                row['level3'] = ''
            if node.level == 3:
                row['level1'] = ''
                row['level2'] = ''
                row['level3'] = node.name + ' [' + ("%0.1f" % node.rawvalue) + '%]'
            row['executing'] = node.executing
            row['pledged'] = node.pledged
            row['delta'] = node.delta
            row['queued'] = node.queued
            row['ratio'] = node.ratio
            row['value'] = node.value
            rows.append(row)
    for item in node.children:
        stripTree(item, rows)


def getChildStat(node, hs_distribution_dict, level):
    executing = 0
    pledged = 0
    delta = 0
    queued = 0
    ratio = 0
    if node.name in hs_distribution_dict and len(node.children) == 0:
        executing = hs_distribution_dict[node.name]['executing']
        pledged = hs_distribution_dict[node.name]['pledged']
        delta = hs_distribution_dict[node.name]['executing'] - hs_distribution_dict[node.name]['pledged']
        queued = hs_distribution_dict[node.name]['queued']
    else:
        for item in node.children:
            getChildStat(item, hs_distribution_dict, level+1)
            executing += item.executing
            pledged += item.pledged
            delta += item.delta
            queued += item.queued
            #ratio = item.ratio if item.ratio!=None else 0

    node.executing = executing
    node.pledged = pledged
    node.delta = delta
    node.queued = queued
    node.level = level

    if (pledged != 0):
        ratio = executing / pledged *100
    else:
        ratio = None

    node.ratio = ratio