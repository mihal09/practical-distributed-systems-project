from datetime import datetime


def parse_timestamp(ts):
    if ts.endswith('Z'):
        ts = ts[:-1]
    if '.' in ts:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")


def is_within_time_range(event_time, start_time, end_time):
    # event_dt = datetime.strptime(event_time[:-1], '%Y-%m-%dT%H:%M:%S.%f')
    event_dt = parse_timestamp(event_time[:-1])
    return start_time <= event_dt < end_time


def remove_nones(dict_object):
    return {k: v for k, v in dict_object.items() if v is not None}
