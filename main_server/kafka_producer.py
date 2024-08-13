import random
import time

from kafka import KafkaProducer
from connections import KafkaClient

BOOTSTRAP_SERVERS = 'localhost:9092'

# producer = KafkaProducer(bootstrap_servers='localhost:9092',
#     compression_type="snappy"
#     linger_ms=5000
# )
# counter = 1


# for i in range(3):
#     for account_number in range(1):
#         transaction_value = counter
#         producer.send('transactions', value=b'%d' % transaction_value, partition=account_number)
#         print(f'new transaction: value={transaction_value} account_number={account_number}')
#         counter += 1
# time.sleep(1)        
kafka_client = KafkaClient()
kafka_client.send(topic="user_tags", value={"elo": {"siema": "eniu"}}, key="key")
print(f"Sent!")