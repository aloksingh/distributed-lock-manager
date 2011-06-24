package com.alok.lock;

import java.util.*;

public interface LockStore {

    UUID add(String lockType, Map<String, String> properties);

    UUID lock(String lockType, String clientId, int ttlInSeconds);

    void complete(UUID jobId, String clientId);

    void release(UUID jobId, String clientId);

    void error(UUID jobId, String lockType, String clientId, String message);

    Collection<UUID> findLocksByType(String lockType, String workerId, int minJobs);

    LockInfo getLock(UUID uuid);

    Set<LockInfo> locksByType(String lockType);


    public static class LockInfo {
        private final UUID id;
        private final String lockType;
        private final String lockedBy;
        private final String slot;
        private final Date lockedAt;
        public static final LockInfo NULL_LOCK = new LockInfo(null, "", "", "", null, null, Collections.<String, String>emptyMap());
        private Map<String, String> properties;
        private Date createdAt;

        public LockInfo(UUID id, String lockType, String slot, String lockedBy, Date createdAt, Date lockedAt, Map<String, String> properties) {
            this.slot = slot;
            this.lockedBy = lockedBy;
            this.lockType = lockType;
            this.id = id;
            this.createdAt = createdAt;
            this.lockedAt = lockedAt;
            this.properties = properties;
        }

        public UUID getId() {
            return id;
        }

        public String getLockType() {
            return lockType;
        }

        public String getLockedBy() {
            return lockedBy;
        }

        public String getSlot() {
            return slot;
        }

        @Override
        public String toString() {
            return "LockInfo{" +
                    "id=" + id +
                    ", lockType='" + lockType + '\'' +
                    ", lockedBy='" + lockedBy + '\'' +
                    ", slot='" + slot + '\'' +
                    ", lockedAt=" + lockedAt +
                    ", createdAt=" + createdAt +
                    ", properties=" + properties +
                    '}';
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public Date getLockedAt() {
            return lockedAt;
        }

        public Date getCreatedAt() {
            return createdAt;
        }
    }
}
