import aerospike
from aerospike import exception as ex
import json
from kafka import KafkaProducer

AEROSPIKE_NAMESPACE = 'mimuw'
AEROSPIKE_SET_NAME = 'tags'
AEROSPIKE_HOSTS = ["aerospikedb"]
AEROSPIKE_PORT=3000

KAFKA_BOOTSTRAP_SERVERS = 'broker-1:19092,broker-2:19092'


class AerospikeClient:
    def __init__(self, hosts=AEROSPIKE_HOSTS, port=AEROSPIKE_PORT, namespace=AEROSPIKE_NAMESPACE, set_name=AEROSPIKE_SET_NAME):
        config = {
            'hosts': [ (host, port) for host in hosts ],
            'policies': {'read': {'total_timeout': 1000}},
        }
        print(f"[AEROSPIKE] {AEROSPIKE_HOSTS},{AEROSPIKE_PORT}")
        self.client = aerospike.client(config).connect()
        self.namespace = namespace
        self.set_name = set_name


    def push_key_value(self, key, value, namespace=None, set_name=None):
        if namespace is None:
            namespace = self.namespace
        if set_name is None:
            set_name = self.set_name

        aerospike_key = (namespace, set_name, key)
        self.client.put(aerospike_key, {'value': value})


    def read_key_value(self, key, namespace=None, set_name=None, default_factory=None):
        if namespace is None:
            namespace = self.namespace
        if set_name is None:
            set_name = self.set_name

        try:
            aerospike_key = (namespace, set_name, key)
            
            (key, metadata, record) = self.client.get(aerospike_key)
            # print(f"Read: {key} -> {record}")
            if len(record) == 1 and 'value' in record:
                return record['value']
            else:
                return record
        except aerospike.exception.RecordNotFound:
            if default_factory:
                return default_factory()
            return None

    def operate(self, key, operations, namespace=None, set_name=None):
        if namespace is None:
            namespace = self.namespace
        if set_name is None:
            set_name = self.set_name

        aerospike_key = (namespace, set_name, key)
        self.client.operate(aerospike_key, operations)

    def clear_setname(self, namespace=None, set_name=None):
        if namespace is None:
            namespace = self.namespace

        if set_name is None:
            set_name = self.set_name

        scan = self.client.scan(namespace, set_name)
        keys = []

        scan.foreach(lambda x: keys.append(x[0]))

        for key in keys:
            try:
                self.client.remove(key)
            except ex.RecordError as e:
                print(f"Failed to delete record with key {key}: {e}")


def serializer(v):
    return json.dumps(v).encode('utf-8')


class KafkaClient():
    @staticmethod
    def serializer(v):
        return json.dumps(v).encode('utf-8')
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, compression_type="snappy", linger_ms=500, serialzier=serializer, *args, **kwargs):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            compression_type=compression_type,
            linger_ms=linger_ms,
            key_serializer=KafkaClient.serializer,
            value_serializer=KafkaClient.serializer,
            api_version = (3,8,0),
            acks=1,
            *args, **kwargs
        )

    def send(self, topic, key=None, value=None):
        self.producer.send(topic, key=key, value=value)
        #self.producer.flush()
