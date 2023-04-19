"""

"""
import re

from django.db.models import Case, When, Value, Sum
from core.globalshares.models import JobsShareStats

def get_child_elements(tree,childsgsharelist):
    for gshare in tree:
        if gshare!='childlist':
            shortGshare = re.sub('\[(.*)\]', '', gshare).rstrip()
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
                gs_name = re.sub('\[(.*)\]', '', l1).rstrip()
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
                    gs_name = re.sub('\[(.*)\]', '', l2).rstrip()
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
                        gs_name = re.sub('\[(.*)\]', '', l3).rstrip()
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
