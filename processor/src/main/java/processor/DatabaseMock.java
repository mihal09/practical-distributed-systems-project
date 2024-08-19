package processor;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Bin;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Operation;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


public class DatabaseMock {
    private static final String AEROSPIKE_NAMESPACE = "mimuw";
    private static final String AEROSPIKE_SET_NAME = "aggregates";
    private static final String AEROSPIKE_HOST = "aerospikedb";
    private static final int AEROSPIKE_PORT = 3000;

    private final AerospikeClient client;
    private final Map<String, UserProfile> userProfileMap;

    public DatabaseMock() {
        this.client = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
        this.userProfileMap = new HashMap<>();
    }

    public UserProfile getUserProfile(String key) {
        return userProfileMap.getOrDefault(key, new UserProfile(key, 0L, 0L));
    }


    public void batchUpdate(List<UserProfile> profiles) {
        BatchPolicy batchPolicy = new BatchPolicy();
        WritePolicy writePolicy = new WritePolicy();
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        List<BatchRecord> batchWrites = new ArrayList<>();

        profiles.forEach(profile -> {
            // System.out.println("Processing key" + profile.key.toString());
            UserProfile existingProfile = userProfileMap.getOrDefault(profile.key, new UserProfile(profile.key, 0L, 0L));

            // System.out.println("Old values:" + existingProfile.count + ", " + existingProfile.sum);
            existingProfile.count += profile.count;
            existingProfile.sum += profile.sum;
            userProfileMap.put(profile.key, existingProfile);
            // System.out.println("New values:" + existingProfile.count + ", " + existingProfile.sum);
            // System.out.println();

            Key aerospikeKey = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET_NAME, profile.key);
            Bin countBin = new Bin("count", Value.get(existingProfile.count));
            Bin sumPriceBin = new Bin("sum_price", Value.get(existingProfile.sum));
            Operation[] operations = Operation.array(
                Operation.put(countBin),
                Operation.put(sumPriceBin)
            );

            BatchWrite batchWrite = new BatchWrite(aerospikeKey, operations);
            batchWrites.add(batchWrite);
         });

        System.out.println("Updated number of records in Aerospike: " + profiles.size());

        client.operate(batchPolicy, batchWrites);

        // } catch (AerospikeException e) {
        //     e.printStackTrace();
        // }
    }

    public static class UserProfile {
        private final String key;
        private long count;
        private long sum;

        public UserProfile(String key, long count, long sum) {
            this.key = key;
            this.count = count;
            this.sum = sum;
        }
    }
}
