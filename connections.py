from pymongo import MongoClient, ASCENDING, DESCENDING
import redis


def get_redis_client(host='localhost', port=6379, db=0, *args, **kwargs):
    redis_client = redis.Redis(host=host, port=port, db=db, *args, **kwargs)
    return redis_client


def get_aggregate_collection(url="mongodb://localhost:27017/", clear_data=False, redis_client=None):
    # Connect to MongoDB
    mongo_client = MongoClient(url)
    db = mongo_client['allezon']
    aggregates_collection = db["aggregate_collection"]

    if clear_data:
        print('Clearing data!')
        aggregates_collection.drop()  # Clear mongo
        for key in redis_client.scan_iter("*"):  # Clear redis
            redis_client.delete(key)
        print('Cleared data!')


    if 'aggregate_collection' not in db.list_collection_names():
        # Create the collection
        agg_col = db.create_collection('aggregate_collection')

        # Create indexes for optimizing queries
        # Indexing time and action fields for faster retrieval based on common query patterns
        agg_col.create_index([("time", DESCENDING), ("action", ASCENDING)])
        agg_col.create_index([("time", DESCENDING), ("action", ASCENDING), ("brand_id", ASCENDING)])
        agg_col.create_index([("time", DESCENDING), ("action", ASCENDING), ("category_id", ASCENDING)])
        agg_col.create_index([("time", DESCENDING), ("action", ASCENDING), ("origin", ASCENDING)])

        # agg_col.create_index([("time", ASCENDING), ("action", ASCENDING), ("brand_id", ASCENDING), ("category_id", ASCENDING), ("origin", ASCENDING)])
        # agg_col.create_index([("time", ASCENDING), ("action", ASCENDING), ("brand_id", ASCENDING), ("origin", ASCENDING), ("category_id", ASCENDING)])
        # agg_col.create_index([("time", ASCENDING), ("action", ASCENDING), ("category_id", ASCENDING), ("brand_id", ASCENDING), ("origin", ASCENDING)])
        # agg_col.create_index([("time", ASCENDING), ("action", ASCENDING), ("category_id", ASCENDING), ("origin", ASCENDING), ("brand_id", ASCENDING)])
        # agg_col.create_index([("time", ASCENDING), ("action", ASCENDING), ("origin", ASCENDING), ("brand_id", ASCENDING), ("category_id", ASCENDING)])
        # agg_col.create_index([("time", ASCENDING), ("action", ASCENDING), ("origin", ASCENDING), ("category_id", ASCENDING), ("brand_id", ASCENDING)])

        print("Aggregate collection created and indexed.")
    else:
        print("Aggregate collection already exists.")

    return aggregates_collection
