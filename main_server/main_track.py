from fastapi import FastAPI, Body, Query, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from typing import List, Optional, Annotated
from pydantic import BaseModel
from datetime import datetime
from prometheus_client import make_asgi_app, Histogram
from connections import AerospikeClient, KafkaClient
from utils import is_within_time_range, generate_query_keys


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
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

ha = Histogram('aerospike_request_latency_seconds', 'Aerospike put latency')
hk = Histogram('kafka_request_latency_seconds', 'Kafka send latency')

aerospike_client = AerospikeClient()
kafka_client = KafkaClient()

async def write_aero_kafka(key, tags, serialized_tag):
    # Send to kafka topic
    aerospike_client.push_key_value(key=key, value=tags)
    kafka_client.send(topic="user_tags", key=key, value=serialized_tag)


@app.post('/user_tags', status_code = 204)
#async def add_user_tag(user_tag : UserTag, background_tasks: BackgroundTasks):
def add_user_tag(user_tag : UserTag):
    # Create keys based on cookie and action
    key = f'{user_tag.cookie}:{user_tag.action.lower()}'

    # Store user tag in Aerospike list and trim the list to the most recent 200 items

    # Aerospike
    tags = aerospike_client.read_key_value(key=key)
    serialized_tag = jsonable_encoder(user_tag)
    if not tags:
        tags = [serialized_tag]
    else:
        tags.insert(0, serialized_tag)
        tags = tags[:200]

#    background_tasks.add_task(write_aero_kafka, key, tags, serialized_tag)
    with ha.time():
        aerospike_client.push_key_value(key=key, value=tags)

    # Send to kafka topic
    with hk.time():
        kafka_client.send(topic="user_tags", key=key, value=serialized_tag)

    return ''

async def read_aero(key):
    return aerospike_client.read_key_value(key=key, default_factory=list)

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
    user_views = [x for x in user_views if is_within_time_range(x['time'], start_time, end_time)][:limit]
    user_buys = [x for x in user_buys if is_within_time_range(x['time'], start_time, end_time)][:limit]


    # Return user profile data
    response = {
        'cookie': cookie,
        'views': user_views,
        'buys': user_buys
    }


    # assert response == expected_result, f"Expected result: {expected_result}\ngot\n{response}\n\n"

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



