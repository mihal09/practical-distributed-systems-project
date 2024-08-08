from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import json
import time
import logging
from logging.handlers import RotatingFileHandler

from classes import AggregatesQueryResult
from connections import get_aggregate_collection, get_redis_client
from utils import is_within_time_range, parse_timestamp, remove_nones


app = Flask(__name__)
DEBUG = False
CLEAR_DATA = 0

# Logging
log = logging.getLogger('werkzeug')
log.disabled = True

handler = RotatingFileHandler('logs.txt', maxBytes=10000, backupCount=3)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

def log_response_time(f):
    """Decorator to log the response time of a function."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        app.logger.info(f"{request.path} took {duration:.4f} seconds")
        return result
    return wrapper



redis_client = get_redis_client()  # Connect to Redis
aggregates_collection = get_aggregate_collection(clear_data=CLEAR_DATA, redis_client=redis_client)  # Connect to mongodb and get the collection


@app.route('/user_tags', methods=['POST'])
def add_user_tag():
    data = request.get_json()
    try:
        user_tag = request.get_json()
        cookie = user_tag.get('cookie')
        action = user_tag.get('action')

        # Create keys based on cookie and action
        key = f'{cookie}:{action.lower()}'

        # Store user tag in Redis list and trim the list to the most recent 200 items
        if DEBUG:
            print(f'[REDIS] Setting key = {key}')

        # Redis
        redis_client.lpush(key, json.dumps(user_tag))
        redis_client.ltrim(key, 0, 199)

        # Mongodb
        user_tag['time'] = parse_timestamp(user_tag['time'])
        product_info = user_tag.pop('product_info')
        user_tag.update(product_info)

        aggregates_collection.insert_one(user_tag)

        cutoff_time = user_tag['time'] - timedelta(hours=24)
        aggregates_collection.delete_many({"time": {"$lt": cutoff_time}})

        # producer.send('user_tags', data)
        # producer.flush()  # Ensure data is sent
        return '', 204  #
    except Exception as e:
        print(f'Got error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/user_profiles/<cookie>', methods=['POST'])
def get_user_profile(cookie):
    time_range = request.args.get('time_range', type=str)
    start_time_str, end_time_str = time_range.split('_')
    start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f')
    end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%f')

    limit = request.args.get('limit', 200, type=int)

     # Fetch user profile data from Redis
    views_key = f'{cookie}:view'
    buys_key = f'{cookie}:buy'
    user_views = redis_client.lrange(views_key, 0, limit - 1)
    user_buys = redis_client.lrange(buys_key, 0, limit - 1)


    # Convert JSON strings back to dictionaries
    user_views = [json.loads(v) for v in user_views if is_within_time_range(json.loads(v)['time'], start_time, end_time)]
    user_buys = [json.loads(b) for b in user_buys if is_within_time_range(json.loads(b)['time'], start_time, end_time)]

    # Return user profile data
    response = {
        'cookie': cookie,
        'views': user_views,
        'buys': user_buys
    }

    expected_result = request.get_json(silent=True)
    if DEBUG:
        print(f"Received request for user profile with cookie {cookie} and time range {time_range} and limit {limit}")
        print(f'User views: {user_views}')
        print(f"Expected result: {expected_result}\ngot\n{response}\n\n")

    # assert response == expected_result

    return jsonify(response)

@app.route('/aggregates', methods=['POST'])
@log_response_time
def get_aggregates():
    time_range = request.args.get('time_range')
    action = request.args.get('action')
    aggregates = request.args.getlist('aggregates')
    origin = request.args.get('origin', default=None)
    brand_id = request.args.get('brand_id', default=None)
    category_id = request.args.get('category_id', default=None)

    start_time_str, end_time_str = time_range.split('_')
    start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S')
    end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S')

    match_stage = {
        '$match': {
            'time': {'$gte': start_time, '$lt': end_time},
            'action': action
        }
    }
    if origin:
        match_stage['$match']['origin'] = origin
    if brand_id:
        match_stage['$match']['brand_id'] = brand_id
    if category_id:
        match_stage['$match']['category_id'] = category_id

    group_stage = {
        '$group': {
            '_id': {
                '1m_bucket': {'$dateToString': {'format': '%Y-%m-%dT%H:%M:00', 'date': '$time', 'timezone': 'UTC'}},
                'action': '$action',
                'brand_id': '$brand_id' if brand_id else None,
                'category_id': '$category_id' if category_id else None,
                'origin': '$origin' if origin else None
            },
            'count': {'$sum': 1} if 'COUNT' in aggregates else None,
            'sum_price': {'$sum': '$price'} if 'SUM_PRICE' in aggregates else None
        }
    }

    group_stage['$group'] = remove_nones(group_stage['$group'])
    group_stage['$group']['_id'] = remove_nones(group_stage['$group']['_id'])

    project_stage = {
        '$project': {
            '_id': 0,
            '1m_bucket': '$_id.1m_bucket',
            'action': '$_id.action',
            'brand_id': '$_id.brand_id',
            'category_id': '$_id.category_id',
            'origin': '$_id.origin',
            'count': 1 if 'COUNT' in aggregates else None,
            'sum_price': 1 if 'SUM_PRICE' in aggregates else None
        }
    }
    project_stage['$project'] = remove_nones(project_stage['$project'])

    sort_stage = {
        '$sort': {'1m_bucket': 1}  # Sorting by '1m_bucket' in ascending order
    }

    # Execute aggregation pipeline
    pipeline = [match_stage, group_stage, project_stage, sort_stage]
    results = list(aggregates_collection.aggregate(pipeline))

    target_output = ""
    data = request.get_json()
    if data:
        aggregates_result = AggregatesQueryResult(
            columns=data.get('columns', []),
            rows=data.get('rows', [])
        )
        target_output = aggregates_result.__dict__

    
    key_columns = ['1m_bucket', 'action']
    if origin:
        key_columns.append('origin')
    if brand_id:
        key_columns.append('brand_id')
    if category_id:
        key_columns.append('category_id')
        
    agg_columns = []
    if 'COUNT' in aggregates:
        agg_columns.append('count')
    if 'SUM_PRICE' in aggregates:
        agg_columns.append('sum_price')

    results_dict = {}
    for item in results:
        key = tuple(str(item[col]) for col in key_columns if item[col] is not None)
        vals = [str(item[col]) if item[col] is not None else None for col in agg_columns]
        results_dict[key] = vals

    total_minutes = int((end_time - start_time).total_seconds() / 60)
    all_minutes = [start_time + timedelta(minutes=i) for i in range(total_minutes)]

    rows = []
    for minute in all_minutes:
        bucket = minute.strftime('%Y-%m-%dT%H:%M:00')
        key = tuple(x for x in [bucket, action, origin, brand_id, category_id] if x is not None)

        if key in results_dict:
            vals = results_dict.get(key)
        else:
            vals = ['0' for _ in agg_columns]

        rows.append(list(key) + vals)


    final_results = {
        'columns': key_columns+agg_columns,
        'rows': rows
    }   

    print(f'Time range: {start_time} - {end_time}')
    print(f'Expected result\n{target_output}\ngot\n{final_results}\n\n')

    assert target_output == final_results, time_range


    return jsonify(final_results)


@app.route('/test', methods=['GET'])
def test():
    # Here you would handle the user tag
    print(f"Testing.")
    return '', 204


if __name__ == '__main__':
    app.run(debug=True, port=5000, host="0.0.0.0")
