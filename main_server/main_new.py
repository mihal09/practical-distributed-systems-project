from flask import Flask, request, jsonify
from datetime import datetime
import json
import time
import logging
from logging.handlers import RotatingFileHandler

from classes import AggregatesQueryResult
from connections import AerospikeClient, KafkaClient
from utils import is_within_time_range, parse_timestamp, remove_nones, generate_query_keys


app = Flask(__name__)
DEBUG = False
# CLEAR_DATA = 0

# Logging
log = logging.getLogger('werkzeug')
log.disabled = True

handler = RotatingFileHandler('logs.txt', maxBytes=10000, backupCount=3)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)


aerospike_client = AerospikeClient()
kafka_client = KafkaClient()


@app.route('/user_tags', methods=['POST'])
def add_user_tag():
    data = request.get_json()
    try:
        user_tag = request.get_json()
        cookie = user_tag.get('cookie')
        action = user_tag.get('action')

        # Create keys based on cookie and action
        key = f'{cookie}:{action.lower()}'

        # Store user tag in Aerospike list and trim the list to the most recent 200 items
        if DEBUG:
            print(f'[AEROSPIKE] Setting key = {key}')

        # Aerospike
        tags = aerospike_client.read_key_value(key=key)
        if not tags:
            tags = [user_tag]
        else:
            tags.insert(0, user_tag)
            tags = tags[:200]

        aerospike_client.push_key_value(key=key, value=tags)

        # Send to kafka topic
        kafka_client.send(topic="user_tags", key=key, value=user_tag)

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

    user_views = aerospike_client.read_key_value(key=views_key, default_factory=list)
    user_buys = aerospike_client.read_key_value(key=buys_key, default_factory=list)


    # Convert JSON strings back to dictionaries
    user_views = [x for x in user_views if is_within_time_range(x['time'], start_time, end_time)][:limit]
    user_buys = [x for x in user_buys if is_within_time_range(x['time'], start_time, end_time)][:limit]


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

    # assert response == expected_result, f"Expected result: {expected_result}\ngot\n{response}\n\n"

    return jsonify(response)


@app.route('/aggregates', methods=['POST'])
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

    keys = generate_query_keys(start_time, end_time, action, origin, brand_id, category_id)
    results = []

    for key in keys:
        try:
            record = aerospike_client.read_key_value(set_name='aggregates', key=key) or {}
            unix_timestamp = int(key.split('|')[0])  # 1m_bucket (epoch seconds)
            dt = datetime.utcfromtimestamp(unix_timestamp)
            bucket = dt.strftime('%Y-%m-%dT%H:%M:%S')
            result_row = list(x for x in [bucket, action, origin, brand_id, category_id] if x is not None)

            if 'COUNT' in aggregates:
                result_row.append(str(record.get('count', 0)))
            if 'SUM_PRICE' in aggregates:
                result_row.append(str(record.get('sum_price', 0)))

            results.append(result_row)
        except aerospike_exception.RecordNotFound:
            continue
        except Exception as e:
            return jsonify({"error": str(e)}), 500

   
    # get results
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


    final_results = {
        'columns': key_columns+agg_columns,
        'rows': results
    }   

    # print(f'Time range: {start_time} - {end_time}')
    # print(f'Expected result\n{target_output}\ngot\n{final_results}\n\n')

    assert target_output == final_results, f'Expected result\n{target_output}\ngot\n{final_results}\n\n'

    return jsonify(final_results)


if __name__ == '__main__':
    app.run(debug=True, port=5000, host="0.0.0.0")
