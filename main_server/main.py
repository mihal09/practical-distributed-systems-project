from fastapi import FastAPI, Body, Query, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from typing import List, Optional, Annotated
from pydantic import BaseModel
from datetime import datetime

from connections import AerospikeClient, KafkaClient
from utils import parse_timestamp, is_within_time_range, generate_query_keys

import aerospike
from aerospike_helpers.operations import list_operations



class ProductInfo(BaseModel):
    product_id : int
    brand_id : str
    category_id : str
    price : int

class UserTag(BaseModel):
    time : str
    cookie : str
    country : str
    device : str
    action : str
    origin : str
    product_info : ProductInfo


class UserProfileResult(BaseModel):
    cookie : str
    views : List[UserTag]
    buys : List[UserTag]

class AggregatesResult(BaseModel):
    columns : list[str]
    rows : list[list[str]]



app = FastAPI()
aerospike_client = AerospikeClient()
kafka_client = KafkaClient()

async def write_aero_kafka(key, serialized_tag):
    aerospike_client.extend_list(key, serialized_tag, max_length=200, max_retries=3)

    # Send to kafka topic
    kafka_client.send(topic="user_tags", key=key, value=serialized_tag)


@app.post('/user_tags', status_code = 204)
#async def add_user_tag(user_tag : UserTag, background_tasks: BackgroundTasks):
def add_user_tag(user_tag : UserTag, background_tasks : BackgroundTasks):
    # Create keys based on cookie and action
    key = f'{user_tag.cookie}:{user_tag.action.lower()}'

    # Store user tag in Aerospike list and trim the list to the most recent 200 items
    serialized_tag = jsonable_encoder(user_tag)

    background_tasks.add_task(write_aero_kafka, key, serialized_tag)
    # write_aero_kafka(key, serialized_tag)
    # kafka_client.send(topic="user_tags", key=key, value=serialized_tag)

    return ''


@app.post('/user_profiles/{cookie}', status_code = 200)
def get_user_profile(cookie : str,
                           time_range : str,
                           expected_result : UserProfileResult,
                           limit : int = 200,
                           ):
    start_time_str, end_time_str = time_range.split('_')
    start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f')
    end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%f')

    views_key = f'{cookie}:view'
    buys_key = f'{cookie}:buy'

    user_views = aerospike_client.read_key_value(key=views_key, default_factory=list)
    user_buys = aerospike_client.read_key_value(key=buys_key, default_factory=list)

    # Convert JSON strings back to dictionaries
    user_views = [x for x in user_views if is_within_time_range(x['time'], start_time, end_time)]
    user_buys = [x for x in user_buys if is_within_time_range(x['time'], start_time, end_time)]

    # user_views.sort(key=lambda x: parse_timestamp(x['time']), reverse=True)
    # user_buys.sort(key=lambda x: parse_timestamp(x['time']), reverse=True)

    user_views = user_views[:limit]
    user_buys = user_buys[:limit]


    # Return user profile data
    response = {
        'cookie': cookie,
        'views': user_views,
        'buys': user_buys
    }

    # target_json = expected_result.model_dump(mode='json')
    # if response['views'] != target_json['views']:
    #     print(f"Expected len: {len(target_json['views'])}, got {len(response['views'])}")
    #     print(f"Expected result:")
    #     for x in target_json['views']:
    #         print(x)
    #     print("\ngot\n")
    #     for x in response['views']:
    #         print(x)


    #return jsonable_encoder(response)
    return response


@app.post("/aggregates", status_code = 200)
def get_aggregates(time_range : str,
                         action : str,
                         aggregates : Annotated[list[str], Query()],
                         origin : Optional[str] = None,
                         brand_id : Optional[str] = None,
                         category_id : Optional[str] = None):

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

   
    # get results
    #target_output = jsonable_encoder(request)

    
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

    #assert target_output == final_results, f'Expected result\n{target_output}\ngot\n{final_results}\n\n'

    return final_results



