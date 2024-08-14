from datetime import datetime, timedelta


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


def generate_query_keys(start_time, end_time, action, origin=None, brand_id=None, category_id=None):
    if origin is None:
        origin = ""
    if brand_id is None:
        brand_id = ""
    if category_id is None:
        category_id = ""

    keys = []
    time_cursor = start_time
    while time_cursor <= end_time:
        key = f"{int(time_cursor.timestamp())}|{action}|{origin}|{brand_id}|{category_id}"
        keys.append(key)
        time_cursor += timedelta(minutes=1)
    return keys


# def log_response_time(f):
#     """Decorator to log the response time of a function."""
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = f(*args, **kwargs)
#         end_time = time.time()
#         duration = end_time - start_time
#         app.logger.info(f"{request.path} took {duration:.4f} seconds")
#         return result
#     return wrapper