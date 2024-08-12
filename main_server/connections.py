from pymongo import MongoClient, ASCENDING, DESCENDING
import aerospike

AEROSPIKE_NAMESPACE = 'mimuw'
AEROSPIKE_SET_NAME = 'tags'
AEROSPIKE_HOSTS = ["st108vm102.rtb-lab.pl"]
AEROSPIKE_PORT=3000


class AerospikeClient:
    def __init__(self, hosts=AEROSPIKE_HOSTS, port=AEROSPIKE_PORT, namespace=AEROSPIKE_NAMESPACE, set_name=AEROSPIKE_SET_NAME):
        config = {
            'hosts': [ (host, port) for host in hosts ],
            'policies': {'read': {'total_timeout': 1000}},
        }
        self.client = aerospike.client(config)
        self.namespace = namespace
        self.set_name = set_name


    def push_key_value(self, key, value, namespace=None, set_name=None):
        if namespace is None:
            namespace = self.namespace
        if set_name is None:
            set_name = self.set_name

        try:
            aerospike_key = (namespace, set_name, key)
            
            self.client.put(aerospike_key, {'value': value})
            # print(f"Successfully written: {key} -> {value}")
        except Exception as e:
            print(f"Error: {e}")

    def read_key_value(self, key, namespace=None, set_name=None, default_factory=None):
        if namespace is None:
            namespace = self.namespace
        if set_name is None:
            set_name = self.set_name

        try:
            aerospike_key = (namespace, set_name, key)
            
            (key, metadata, record) = self.client.get(aerospike_key)
            # print(f"Read: {key} -> {record['value']}")
            return record['value']
        except aerospike.exception.RecordNotFound:
            if default_factory:
                return default_factory()
            return None

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
