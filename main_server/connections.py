import aerospike
from aerospike import exception as ex
import json
from kafka import KafkaProducer

AEROSPIKE_NAMESPACE = 'mimuw'
AEROSPIKE_SET_NAME = 'tags'
AEROSPIKE_HOSTS = [f"st108vm1{x:02d}.rtb-lab.pl" for x in range(6, 10+1)]
AEROSPIKE_PORT=3000

KAFKA_BOOTSTRAP_SERVERS = 'st108vm102.rtb-lab.pl:9092'


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
            # print(f"Read: {key} -> {record}")
            if len(record) == 1 and 'value' in record:
                return record['value']
            else:
                return record
        except aerospike.exception.RecordNotFound:
            if default_factory:
                return default_factory()
            return None

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
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, compression_type="snappy", linger_ms=5000, serialzier=serializer, *args, **kwargs):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            compression_type=compression_type,
            linger_ms=linger_ms,
            key_serializer=serializer,
            value_serializer=serializer,
            *args, **kwargs
        )

    def send(self, topic, key=None, value=None):
        self.producer.send(topic, key=key, value=value)
        self.producer.flush()