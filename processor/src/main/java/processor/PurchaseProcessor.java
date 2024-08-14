package processor;

import processor.domain.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class PurchaseProcessor implements Processor<String, String, String, String> {
    private static final String COUNT_STORE_NAME = "purchases-count";
    private static final String SUM_STORE_NAME = "purchases-sum";
    private static final Duration SCHEDULE_INTERVAL = Duration.ofSeconds(15);

    private KeyValueStore<String, Long> countStore;
    private KeyValueStore<String, Long> sumStore;
    private DatabaseMock database;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(ProcessorContext<String, String> context) {
        objectMapper.registerModule(new JavaTimeModule());
        countStore = context.getStateStore(COUNT_STORE_NAME);  // TODO: have only one store storing a class AggregatedMetrics
        sumStore = context.getStateStore(SUM_STORE_NAME);
        database = new DatabaseMock();
        context.schedule(SCHEDULE_INTERVAL, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            List<DatabaseMock.UserProfile> profiles = new ArrayList<>();
            try (final KeyValueIterator<String, Long> iter = countStore.all()) {
                while (iter.hasNext()) {
                    final KeyValue<String, Long> entry = iter.next();
                    String key = entry.key;
                    Long count = entry.value;
                    Long sum = sumStore.get(key);
                    profiles.add(new DatabaseMock.UserProfile(key, count, sum));

                    countStore.put(key, null);
                    sumStore.put(key, null);
                }
            }
            database.batchUpdate(profiles);
        });
    }

    @Override
    public void process(Record<String, String> record) {
        // System.out.println("Processing record " + record.toString());
        UserTagEvent userTagEvent;
        try {
            userTagEvent = objectMapper.readValue(record.value(), UserTagEvent.class);
            // System.out.println("Deserialized tag, product id: " + userTagEvent.getProductInfo().getProductId());
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to deserialize record value: " + record.value());
            return;
        }
        
        try {
            String action = userTagEvent.getAction().toString();
            String origin = userTagEvent.getOrigin();
            String brand_id = userTagEvent.getProductInfo().getBrandId();
            String categoryId = userTagEvent.getProductInfo().getCategoryId();
            int price = userTagEvent.getProductInfo().getPrice();
            Instant eventTime = userTagEvent.getTime();
            long windowStart = eventTime.getEpochSecond() / 60 * 60; // 1-minute window
            List<String> keys = generateAggregationKeys(windowStart, action, origin, brand_id, categoryId);

            // System.out.println("Keys: " + keys);

            for (String aggKey : keys) {
                Long oldCount = countStore.get(aggKey);
                long newCount = oldCount == null ? 1 : oldCount + 1;
                countStore.put(aggKey, newCount);

                Long oldSum = sumStore.get(aggKey);
                long newSum = oldSum == null ? price : oldSum + price;
                sumStore.put(aggKey, newSum);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to process record: " + record.value());
        }
    }

    
    private static List<String> generateAggregationKeys(long windowStart, String action, String origin, String brandId, String categoryId) {
        List<String> keys = new ArrayList<>();

        String baseKey = windowStart + "|" + action;
        keys.add(baseKey);
        if (origin != null) keys.add(baseKey + "|" + origin + "||");
        if (brandId != null) keys.add(baseKey + "||" + brandId + "|");
        if (categoryId != null) keys.add(baseKey + "|||" + categoryId);
        if (origin != null && brandId != null) keys.add(baseKey + "|" + origin + "|" + brandId + "|");
        if (origin != null && categoryId != null) keys.add(baseKey + "|" + origin + "||" + categoryId);
        if (brandId != null && categoryId != null) keys.add(baseKey + "||" + brandId + "|" + categoryId);
        if (origin != null && brandId != null && categoryId != null) keys.add(baseKey + "|" + origin + "|" + brandId + "|" + categoryId);
        return keys;
    }

    
    static class AggregatedMetrics {
        long count;
        long sumPrice;

        AggregatedMetrics() {
            this.count = 0;
            this.sumPrice = 0;
        }

        AggregatedMetrics(long count, long sumPrice) {
            this.count = count;
            this.sumPrice = sumPrice;
        }
    }

}
