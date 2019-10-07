"""
Created on 15.04.2019 by Tatiana Korchuganova
Function to transform data from standard Grafana API response to D3.js compatible
"""


def stacked_hist(series, group_by=None, split_series=None):
    plot_data = {}

    if not group_by:
        return plot_data
    if not split_series:
        split_series_value = 'all'

    split_series_list = []
    tags = series[0]['tags'].keys()
    if group_by in tags:
        for s in series:
            split_series_value = s['tags'][split_series] if split_series else split_series_value
            if split_series_value not in split_series_list:
                split_series_list.append(split_series_value)
            if s['tags'][group_by] not in plot_data:
                plot_data[s['tags'][group_by]] = {}
            if split_series_value not in plot_data[s['tags'][group_by]]:
                plot_data[s['tags'][group_by]][split_series_value] = 0
            plot_data[s['tags'][group_by]][split_series_value] += s['values'][0][1]

    # fill holes by 0 value
    for gb, ssd in plot_data.items():
        for s in split_series_list:
            if s not in ssd.keys():
                ssd[s] = 0

    return plot_data


def pledges_merging(data, pledges, coeff, pledges_dict, type='dst_federation'):
    if type == 'dst_federation':
        pl_type = 'real_federation'
        federations_info = {}
        for fed in data['results'][0]['series']:
            # fed['values'][-1][1] = 0
            if fed['tags'][type] not in federations_info:
                federations_info[fed['tags'][type]] = [{'site': fed['tags']['dst_experiment_site'],
                                                       'computingsite': fed['tags']['computingsite'],
                                                       'tier': fed['tags']['dst_tier'],
                                                       'sum_hs06sec': int(round(float(sum_calculate(fed['values'], 1) / 86400))),
                                                       'sum_count': sum_calculate(fed['values'], 2),
                                                       'sum_cpuconsumptiontime': int(round(float(sum_calculate(fed['values'], 3) / 86400))),
                                                       'sum_walltime': int(round(float(sum_calculate(fed['values'], 4) / 86400)))
                                                       }]
            else:
                federations_info[fed['tags'][type]].append({'site': fed['tags']['dst_experiment_site'],
                                                       'computingsite': fed['tags']['computingsite'],
                                                       'tier': fed['tags']['dst_tier'],
                                                       'sum_hs06sec': int(round(float(sum_calculate(fed['values'], 1) / 86400))),
                                                       'sum_count': sum_calculate(fed['values'], 2),
                                                       'sum_cpuconsumptiontime': int(round(float(sum_calculate(fed['values'], 3) / 86400))),
                                                       'sum_walltime': int(round(float(sum_calculate(fed['values'], 4) / 86400)))
                                                       })
            if fed['tags'][type] not in pledges_dict:
                pledges_dict[fed['tags'][type]] = {}
                pledges_dict[fed['tags'][type]]['tier'] = fed['tags']['dst_tier']
                pledges_dict[fed['tags'][type]]["hs06sec"] = 0
                pledges_dict[fed['tags'][type]]["pledges"] = 0
            for value in fed['values']:
                pledges_dict[fed['tags'][type]]['hs06sec'] += value[1]

        for fed in pledges['results'][0]['series']:
            # fed['values'][-1][1] = 0
            if fed['tags'][pl_type] not in pledges_dict:
                pledges_dict[fed['tags'][pl_type]] = {}
                if fed['tags']['tier'] == 'Tier 0':
                    pledges_dict[fed['tags'][pl_type]]['tier'] = 0
                elif fed['tags']['tier'] == 'Tier 1':
                    pledges_dict[fed['tags'][pl_type]]['tier'] = 1
                elif fed['tags']['tier'] == 'Tier 2':
                    pledges_dict[fed['tags'][pl_type]]['tier'] = 2
                elif fed['tags']['tier'] == 'Tier 3':
                    pledges_dict[fed['tags'][pl_type]]['tier'] = 3
                pledges_dict[fed['tags'][pl_type]]["hs06sec"] = 0
                pledges_dict[fed['tags'][pl_type]]["pledges"] = 0
            for value in fed['values']:
                pledges_dict[fed['tags'][pl_type]]['pledges'] += value[1]
        return pledges_dict, federations_info
    if type == 'dst_country':
        pl_type = 'country'

        for fed in data['results'][0]['series']:
            # fed['values'][-1][1] = 0
            if fed['tags'][type] == "United States of America":
                fed['tags'][type] = "USA"
            if fed['tags'][type] not in pledges_dict:
                if fed['tags']['dst_federation'] in ('CH-CERN'):
                    fed['tags'][type] = 'CERN'
                pledges_dict[fed['tags'][type]] = {}
                pledges_dict[fed['tags'][type]]["hs06sec"] = 0
                pledges_dict[fed['tags'][type]]["pledges"] = 0
                for value in fed['values']:
                    pledges_dict[fed['tags'][type]]['hs06sec'] += value[1]
            else:
                if fed['tags']['dst_federation'] in ('CH-CERN'):
                    fed['tags'][type] = 'CERN'
                for value in fed['values']:
                    pledges_dict[fed['tags'][type]]['hs06sec'] += value[1]

        for fed in pledges['results'][0]['series']:
            # fed['values'][-1][1] = 0
            if fed['tags'][pl_type] not in pledges_dict:
                # fed['values'][1] = fed['values'][2]
                # pledges_dict[fed['tags'][pl_type]]['pledges'] = 0
                if fed['tags'][pl_type] == 'Latin America':
                    fed['tags'][pl_type] = 'Chile'
                if fed['tags']['real_federation'] in ('CH-CERN'):
                    fed['tags'][pl_type] = 'CERN'
                if fed['tags'][pl_type] not in pledges_dict:
                    pledges_dict[fed['tags'][pl_type]] = {}
                    pledges_dict[fed['tags'][pl_type]]["hs06sec"] = 0
                    pledges_dict[fed['tags'][pl_type]]["pledges"] = 0
                    for value in fed['values']:
                        pledges_dict[fed['tags'][pl_type]]['pledges'] += value[1]
            else:
                if fed['tags'][pl_type] == 'Latin America':
                    fed['tags'][pl_type] = 'Chile'
                if fed['tags']['real_federation'] in ('CH-CERN'):
                    fed['tags'][pl_type] = 'CERN'
                for value in fed['values']:
                    pledges_dict[fed['tags'][pl_type]]['pledges'] += value[1]
        return pledges_dict


def sum_calculate(data, column_number):
    sum_for_column = 0
    for value in data:
        sum_for_column += value[column_number]
    return sum_for_column
