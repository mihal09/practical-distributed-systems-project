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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.concurrent.atomic.AtomicLong;


public class PurchaseProcessor implements Processor<String, String, String, String> {
    private static final String COUNT_STORE_NAME = "purchases-count";
    private static final String SUM_STORE_NAME = "purchases-sum";
    private static final Duration SCHEDULE_INTERVAL = Duration.ofSeconds(15);

    private static KeyValueStore<String, Long> countStore;
    private static KeyValueStore<String, Long> sumStore;
    private static DatabaseMock database;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static boolean isInitialized = false;

    @Override
    public void init(ProcessorContext<String, String> context) {
        objectMapper.registerModule(new JavaTimeModule());

        if (!isInitialized){
            isInitialized = true;
            database = new DatabaseMock();
            countStore = context.getStateStore(COUNT_STORE_NAME);  // TODO: have only one store storing a class AggregatedMetrics
            sumStore = context.getStateStore(SUM_STORE_NAME);

            System.out.println("INITIALIZING PROCESSOR");
            context.schedule(SCHEDULE_INTERVAL, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                List<DatabaseMock.UserProfile> profiles = new ArrayList<>();
                LocalDateTime now = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
                String formattedNow = now.format(formatter);
                System.out.println(formattedNow + "| STARTING DUMPING TO AEROSPIKE");
                try (final KeyValueIterator<String, Long> iter = countStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, Long> entry = iter.next();
                        String key = entry.key;
                        countStore.put(key, null); // COMMENT THIS

                        Long count = entry.value;
                        Long sum = sumStore.get(key);
                        sumStore.put(key, null);

                        profiles.add(new DatabaseMock.UserProfile(key, count, sum));

                        // System.out.println("Saving profile:" + key.toString() + ", " + count + ", " + sum);

                    }
                }
                database.batchUpdate(profiles);
            });
        }
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
                synchronized (this) {
                    LocalDateTime now = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
                    String formattedNow = now.format(formatter);

                    Long oldCount = countStore.get(aggKey);
                    long newCount = oldCount == null ? 1 : oldCount + 1;
                    countStore.put(aggKey, newCount);
                    
                    Long sanityNewCount = countStore.get(aggKey);

                    // if (aggKey.equals("1646092860|VIEW||Round_Hill_Furniture|Care_Products")){
                    //     System.out.println(formattedNow + "| [PROCESSING] Old count= " + oldCount + ", new_count= " + newCount + ", sanity_new_count= " + sanityNewCount);
                    // }

                    Long oldSum = sumStore.get(aggKey);
                    long newSum = oldSum == null ? price : oldSum + price;
                    sumStore.put(aggKey, newSum);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to process record: " + record.value());
        }
    }

    
    private static List<String> generateAggregationKeys(long windowStart, String action, String origin, String brandId, String categoryId) {
        List<String> keys = new ArrayList<>();

        String baseKey = windowStart + "|" + action;
        keys.add(baseKey + "|||");
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
