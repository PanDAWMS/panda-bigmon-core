""""""

from core.pandajob.models import Jobsarchived_y2014, Jobsarchived_y2015, Jobsarchived_y2016, Jobsarchived_y2017, \
    Jobsarchived_y2018, Jobsarchived, Jobsarchived4
from core.libs.exlib import parse_datetime
from core.settings.config import DEPLOYMENT


def get_pandajob_models_by_year(timewindow):
    """
    List of PanDA job models
    :return:

    """
    if DEPLOYMENT == "ORACLE_ATLAS":
        pjm_year_dict = {
            2014: [Jobsarchived_y2014, ],
            2015: [Jobsarchived_y2015, ],
            2016: [Jobsarchived_y2016, ],
            2017: [Jobsarchived_y2017, ],
            2018: [Jobsarchived_y2018, ],
            2019: [Jobsarchived, ],
            2020: [Jobsarchived, ],
            2021: [Jobsarchived, ],
            2022: [Jobsarchived, Jobsarchived4],
        }
    else:
        pjm_year_dict = {
            2020: [Jobsarchived, ],
            2021: [Jobsarchived, ],
            2022: [Jobsarchived, Jobsarchived4],
        }
    pandajob_models = []

    if len(timewindow) == 2 and isinstance(timewindow[0], str):
        timewindow = [parse_datetime(t) for t in timewindow]

    for y in range(timewindow[0].year, timewindow[1].year + 1):
        if y in pjm_year_dict:
            pandajob_models.extend(pjm_year_dict[y])

    pandajob_models = list(set(pandajob_models))

    return pandajob_models


def identify_jobtype(list_of_dict, field_name='prodsourcelabel'):
    """
    Translate prodsourcelabel values to descriptive analy|prod job types
    The base param is prodsourcelabel, but to identify which HC test template a job belong to we need transformation.
    If transformation ends with '.py' - prod, if it is PanDA server URL - analy.
    Using this as complementary info to make a decision.
    """

    psl_to_jt = {
        'panda': 'analy',
        'user': 'analy',
        'managed': 'prod',
        'prod_test': 'prod',
        'ptest': 'prod',
        'rc_alrb': 'analy',
        'rc_test2': 'analy',
    }

    if DEPLOYMENT == 'ORACLE_DOMA':
        psl_to_jt = {
            'test': 'prod',
            'ANY': 'prod',
        }

    trsfrm_to_jt = {
        'run': 'analy',
        'py': 'prod',
    }

    new_list_of_dict = []
    for row in list_of_dict:
        if field_name in row and row[field_name] in psl_to_jt.keys():
            row['jobtype'] = psl_to_jt[row[field_name]]
            if 'transform' in row and row['transform'] in trsfrm_to_jt and row[field_name] in ('rc_alrb', 'rc_test2'):
                row['jobtype'] = trsfrm_to_jt[row['transform']]
            new_list_of_dict.append(row)

    return new_list_of_dict
