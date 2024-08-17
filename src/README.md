## Dependencies
- Install the required libraries
```bash
sudo apt-get install libsnappy-dev
pip install flask kafka-python python-snappy
```

- Create kafka topic for events
 ```bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 -partitions 2 --replication-factor 2 --topic user_tags
```

## Start
To start the processor server, run:
```bash
python3 main_new.py
```