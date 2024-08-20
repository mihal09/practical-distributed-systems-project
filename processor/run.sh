/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --list
echo -e 'Creating kafka topics'
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --create --if-not-exists --topic user_tags --replication_factor 2 --partitions 2
echo -e 'Sucessfuly created the following topics:'
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --list

echo -e 'Starting aggregate processor:'
java -jar /opt/processor/target/processor-1.0.jar
