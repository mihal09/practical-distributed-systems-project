package processor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class MyApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "purchases-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // StreamsBuilder builder = new StreamsBuilder();
        // KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC);

        // ObjectMapper objectMapper = new ObjectMapper();

        // // Generating all possible aggregation keys
        // KStream<String, AggregatedMetrics> processedStream = sourceStream
        //     .flatMap((key, value) -> {
        //         List<KeyValue<String, AggregatedMetrics>> result = new ArrayList<>();
        //         try {
        //             JsonNode jsonNode = objectMapper.readTree(value);
        //             String action = jsonNode.get("action").asText();
        //             String origin = jsonNode.has("origin") ? jsonNode.get("origin").asText() : null;
        //             String brandId = jsonNode.get("product_info").has("brand_id") ? jsonNode.get("product_info").get("brand_id").asText() : null;
        //             String categoryId = jsonNode.get("product_info").has("category_id") ? jsonNode.get("product_info").get("category_id").asText() : null;
        //             int price = jsonNode.get("product_info").get("price").asInt();

        //             List<String> keys = generateAggregationKeys(action, origin, brandId, categoryId);

        //             for (String aggKey : keys) {
        //                 AggregatedMetrics metrics = new AggregatedMetrics(1, price);
        //                 result.add(new KeyValue<>(aggKey, metrics));
        //             }
        //         } catch (Exception e) {
        //             e.printStackTrace();
        //         }
        //         return result;
        //     });

        // KTable<Windowed<String>, AggregatedMetrics> aggregatedTable = processedStream
        //     .groupByKey()
        //     .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        //     .aggregate(
        //         AggregatedMetrics::new,
        //         (key, value, aggregate) -> {
        //             aggregate.count += value.count;
        //             aggregate.sumPrice += value.sumPrice;
        //             return aggregate;
        //         },
        //         Materialized.<String, AggregatedMetrics, WindowStore<Bytes, byte[]>>as(STORE_NAME)
        //             .withKeySerde(Serdes.String())
        //             .withValueSerde(new AggregatedMetricsSerde())
        //     );

        // // Define punctuator to periodically dump data to Aerospike
        // builder.addStateStore(Stores.keyValueStoreBuilder(
        //     Stores.inMemoryKeyValueStore(STORE_NAME),
        //     Serdes.String(),
        //     new AggregatedMetricsSerde()
        // ));


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

        Topology builder = new Topology();
        builder.addSource("source", "user_tags")
                .addProcessor("processor", PurchaseProcessor::new, "source")
                .addStateStore(countStoreBuilder, "processor")
                .addStateStore(sumStoreBuilder, "processor");

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
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
