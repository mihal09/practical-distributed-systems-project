import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
counter = 1

for i in range(3):
    for account_number in range(1):
        transaction_value = counter
        producer.send('transactions', value=b'%d' % transaction_value, partition=account_number)
        print(f'new transaction: value={transaction_value} account_number={account_number}')
        counter += 1
time.sleep(1)