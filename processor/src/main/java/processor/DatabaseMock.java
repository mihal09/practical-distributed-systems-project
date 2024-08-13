package processor;

import java.util.List;

public class DatabaseMock {
    public void updateUserProfile(String key, long count, long sum) {
        System.out.println("Updated profile " + key + ", current count = " + count + ", current sum = " + sum);
    }

    public void batchUpdate(List<UserProfile> profiles) {
        for (UserProfile userProfile : profiles) {
            System.out.println("Fast update of " + userProfile.key + " count = "
                    + userProfile.count + " sum = " + userProfile.sum);
        }
        System.out.println("updated " + profiles.size() + " profiles");
    }

    public static class UserProfile {
        private final String key;
        private final long count;
        private final long sum;

        public UserProfile(String key, long count, long sum) {
            this.key = key;
            this.count = count;
            this.sum = sum;
        }
    }
}
