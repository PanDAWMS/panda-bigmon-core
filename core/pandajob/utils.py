""""""

from core.pandajob.models import Jobsarchived_y2014, Jobsarchived_y2015, Jobsarchived_y2016, Jobsarchived_y2017, \
    Jobsarchived_y2018, Jobsarchived, Jobsarchived4
from core.libs.exlib import parse_datetime


def get_pandajob_models_by_year(timewindow):
    """
    List of PanDA job models
    :return:

    """
    pjm_year_dict = {
        2014: [Jobsarchived_y2014, ],
        2015: [Jobsarchived_y2015, ],
        2016: [Jobsarchived_y2016, ],
        2017: [Jobsarchived_y2017, ],
        2018: [Jobsarchived_y2018, ],
        2019: [Jobsarchived, ],
        2020: [Jobsarchived, ],
        2021: [Jobsarchived, Jobsarchived4],
    }
    pandajob_models = []

    if len(timewindow) == 2 and isinstance(timewindow[0], str):
        timewindow = [parse_datetime(t) for t in timewindow]

    for y in range(timewindow[0].year, timewindow[1].year + 1):
        if y in pjm_year_dict:
            pandajob_models.extend(pjm_year_dict[y])

    pandajob_models = list(set(pandajob_models))

    return pandajob_models

