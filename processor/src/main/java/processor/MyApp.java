package processor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class MyApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "purchases-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:19092,broker-2:19092"); // Kafka servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 600000); // 10 minutes
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000); // 60 seconds
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000); // 10 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 20000); // 20 seconds
        props.put(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG, 300000); // 5 minutes


        StoreBuilder<KeyValueStore<String, Long>> countStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("purchases-count"),
                Serdes.String(),
                Serdes.Long()
        );

        StoreBuilder<KeyValueStore<String, Long>> sumStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("purchases-sum"),
                Serdes.String(),
                Serdes.Long()
        );

	System.out.println("Building topology...");
        Topology builder = new Topology();
        builder.addSource("source", "user_tags")
                .addProcessor("processor", PurchaseProcessor::new, "source")
                .addStateStore(countStoreBuilder, "processor")
                .addStateStore(sumStoreBuilder, "processor");

	System.out.println("Sucessfully built topology");
        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();  
            }
        });

        try {
	        System.out.println("Starting streams...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
