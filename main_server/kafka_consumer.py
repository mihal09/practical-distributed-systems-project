from kafka import KafkaConsumer, KafkaProducer

balance = {}
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    enable_auto_commit=False,  # Disable auto-commit
)
# consumer.isolation_level='read_committed'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)
for msg in consumer:

    print(f"got message {msg}")
    # account_number = msg.partition
    # if account_number in balance:
    #     balance[account_number] += int(msg.value)
    # else:
    #     balance[account_number] = int(msg.value)
    # print(f'current balance for account {account_number} is {balance[account_number]}')