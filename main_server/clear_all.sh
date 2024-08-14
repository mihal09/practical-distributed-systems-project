python3 clear_database.py
/opt/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic purchases-aggregator-purchases-count-changelog 
/opt/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic purchases-aggregator-purchases-sum-changelog 