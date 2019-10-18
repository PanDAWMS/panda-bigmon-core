"""
Created by Tatiana Korchuganova on 2019.10.18
Utils for oi module
"""
from datetime import datetime, timedelta


def round_time(date_time=None, time_delta=timedelta(minutes=1)):
    """Round a datetime object to a multiple of a timedelta
    date_time : datetime.datetime object, default is now.
    time_delta : timedelta object, to which date_time should be rounded to, default is 1 minute.
    """

    round_to = time_delta.total_seconds()

    if not date_time:
        date_time = datetime.now()

    seconds = (date_time - date_time.min).seconds
    rounding = (seconds+round_to/2) // round_to * round_to

    return date_time + timedelta(0, rounding-seconds, -date_time.microsecond)