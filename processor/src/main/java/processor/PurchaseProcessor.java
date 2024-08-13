package processor;

import processor.domain.UserTagEvent;
import java.time.Duration;
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
    private KeyValueStore<String, Long> countStore;
    private KeyValueStore<String, Long> sumStore;
    private DatabaseMock database;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(ProcessorContext<String, String> context) {
        objectMapper.registerModule(new JavaTimeModule());
        countStore = context.getStateStore("purchases-count");
        sumStore = context.getStateStore("purchases-sum");
        database = new DatabaseMock();
        context.schedule(Duration.ofSeconds(15), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            List<DatabaseMock.UserProfile> profiles = new ArrayList<>();
            try (final KeyValueIterator<String, Long> iter = countStore.all()) {
                while (iter.hasNext()) {
                    final KeyValue<String, Long> entry = iter.next();
                    String userId = entry.key;
                    Long count = entry.value;
                    Long sum = sumStore.get(userId);
                    profiles.add(new DatabaseMock.UserProfile(userId, count, sum));
                }
            }
            database.batchUpdate(profiles);
        });
    }

    @Override
    public void process(Record<String, String> record) {
        System.out.println("Processing record " + record.toString());

        try {
            UserTagEvent userTagEvent = objectMapper.readValue(record.value(), UserTagEvent.class);
            // Now you can work with userTagEvent object
            System.out.println("Deserialized UserTagEvent: " + userTagEvent);
            System.out.println("Product id: " + userTagEvent.getProductInfo().getProductId());
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to deserialize record value: " + record.value());
        }


        // String[] splitMessage = record.value().split("\\W+");
        // String userId = splitMessage[0];
        // int purchaseValue = Integer.parseInt(splitMessage[1]);

        // Long oldCount = countStore.get(userId);
        // long newCount = oldCount == null ? 1 : oldCount + 1;
        // countStore.put(userId, newCount);

        // Long oldSum = sumStore.get(userId);
        // long newSum = oldSum == null ? purchaseValue : oldSum + purchaseValue;
        // sumStore.put(userId, newSum);
    }
}
