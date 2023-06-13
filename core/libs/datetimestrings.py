"""
A set of function to handle date|time strings
"""
import datetime
from dateutil.parser import parse

from django.conf import settings


def parse_datetime(datetime_str):
    """
    :param datetime_str: datetime str in any format
    :return: datetime value
    """
    try:
        datetime_val = parse(datetime_str)
    except ValueError:
        datetime_val = datetime.utcfromtimestamp(datetime_str)
    return datetime_val


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


def stringify_datetime_fields(input, model):
    """
    Transform datetime to str for all parameters for list of objects based on model description
    :param input: list of dicts or dict
    :param model: Django model
    :return: object_list: updated list of dicts
    """
    datetime_fields = [f.name for f in model._meta.get_fields() if 'date' in str(f.description).lower()]
    if isinstance(input, dict):
        object_list = [input, ]
    elif isinstance(input, list):
        object_list = input
    else:
        object_list = []

    for o in object_list:
        for dtf in datetime_fields:
            if dtf in o and isinstance(o[dtf], datetime.datetime) and o[dtf] is not None:
                o[dtf] = o[dtf].strftime(settings.DATETIME_FORMAT)

    return object_list
