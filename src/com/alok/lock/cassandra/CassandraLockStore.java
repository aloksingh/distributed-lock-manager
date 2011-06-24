package com.alok.lock.cassandra;

import com.alok.lock.LockStore;
import com.alok.lock.util.LRUCache;
import com.alok.lock.util.Util;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;

public class CassandraLockStore extends BaseCassandraStore implements LockStore {
    private static final String CF_LOCK_DETAIL = "lock_detail";
    private static final String CF_LOCK_QUEUE = "lock_queue";
    private static final String CF_LOCK_ERROR_QUEUE = "lock_error_queue";
    private static final String CF_LOCK_TOKEN = "lock_token";
    private static final String CF_LOCK_CHOOSING = "lock_choosing";
    private static final String CF_TYPE_LOCK = "lock_type_lock";
    private static final String COL_LOCK_TYPE = "lock_type";
    private static final String COL_LOCKED_BY = "locked_by";
    private static final String COL_CREATED_AT = "created_at";
    private static final String COL_LOCKED_AT = "locked_at";
    private static final String COL_MESSAGE = "message";
    private static final String COL_TYPE_SLOT = "lock_type_slot";
    public static final List<Integer> SLOTS = new Util().range(0,256);
    private static final LRUCache<String, Long> emptySlots = new LRUCache<String, Long>(100, 5);
    private static final int TTL_CHOOSING_SEC = 5;

    public CassandraLockStore(Properties props) {
        super(props);
    }

    @Override
    public UUID add(String lockType, Map<String, String> properties) {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
        final int slot = slot();
        String lockTypeSlot = lockTypeKey(lockType, slot);
        mutator.addInsertion(uuid, CF_LOCK_QUEUE, HFactory.createStringColumn(COL_LOCK_TYPE, lockType));
        mutator.addInsertion(uuid, CF_LOCK_QUEUE, HFactory.createStringColumn(COL_TYPE_SLOT, "" + slot));
        mutator.addInsertion(uuid, CF_LOCK_QUEUE, HFactory.createStringColumn(COL_CREATED_AT, util.toISOUtcString(System.currentTimeMillis())));
        for (String prop : properties.keySet()) {
            String value = properties.get(prop);
            if(value != null){
                mutator.addInsertion(uuid, CF_LOCK_DETAIL, HFactory.createStringColumn(prop, value));
            }
        }
        addToProcessor(uuid, lockTypeSlot);
        mutator.execute();
        return uuid;
    }

    private void addToProcessor(UUID uuid, String slotKey) {
        Mutator<String> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), SSER);
        mutator.addInsertion(slotKey, CF_TYPE_LOCK, HFactory.createColumn(uuid, 1l, UUIDSER, LSER));
        mutator.execute();

    }

    private Map<String, String> lockProperties(UUID jobId){
        SliceQuery<UUID, String, String> sliceQuery = HFactory.createSliceQuery(keyspace(), UUIDSER, SSER, SSER);
        sliceQuery.setKey(jobId);
        sliceQuery.setColumnFamily(CF_LOCK_DETAIL);
        sliceQuery.setRange("", "", false, Integer.MAX_VALUE);
        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
        ColumnSlice<String, String> slice = result.get();
        List<HColumn<String,String>> columns = slice.getColumns();
        Map<String, String> properties = new HashMap<String, String>();
        for (HColumn<String, String> column : columns) {
            properties.put(column.getName(), column.getValue());
        }
        return properties;
    }

    private String lockTypeKey(String lockType, int slot) {
        return lockType + "." + slot;
    }

    private int slot() {
        return SLOTS.get((int) (Math.random() * (SLOTS.size()*999/1000)));
    }

    @Override
    public UUID lock(String lockType, String workerId, int ttlInSeconds) {
        String requestorId = TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();
        Set<UUID> locks = findLocksByType(lockType, workerId, 1);
//        System.out.println("Jobs found:" + locks.size());
        UUID lock = null;
        while (lock == null && locks.size() > 0){
            UUID attempt = null;
            int index = (int) (Math.random() * (locks.size() - 1));
            attempt = (UUID) locks.toArray()[index];
            locks.remove(attempt);
            List<Long> tokens = requestorTokens(attempt, workerId);
            if(tokens.size() > 2){
                continue;
            }
            long start = System.currentTimeMillis();
            addChoosingFlag(attempt, requestorId);
            tokens = requestorTokens(attempt, workerId);
            Long requestorToken = nextToken(tokens);
            writeToken(attempt, requestorId, requestorToken, ttlInSeconds);
            removeChoosingFlag(attempt, requestorId);
            tokens = requestorTokens(attempt, workerId);
            List<String> clientsInFlight = workersChoosing(attempt);
            while(clientsInFlight.size() > 0 && (System.currentTimeMillis() - start) < 10000){
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                }
                clientsInFlight = workersChoosing(attempt);
            }
            if(clientsInFlight.size() > 0){
                removeToken(attempt, requestorId);
                continue;
            }
            if(requestorToken == smallestToken(requestorTokens(attempt, workerId))){
                //Can aquire the lock
                lock(attempt, workerId, ttlInSeconds);
                return attempt;
            }else{
                removeToken(attempt, requestorId);
            }
        }
        return null;
    }

    private void lock(UUID id, String workerId, int ttlInSeconds) {
        Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
        HColumn<String, String> lockedByCol = HFactory.createStringColumn(COL_LOCKED_BY, workerId);
        HColumn<String, String> lockedByAt = HFactory.createStringColumn(COL_LOCKED_AT, util.toISOUtcString(System.currentTimeMillis()));
        lockedByCol.setTtl(ttlInSeconds);
        mutator.addInsertion(id, CF_LOCK_QUEUE, lockedByCol);
        mutator.addInsertion(id, CF_LOCK_QUEUE, lockedByAt);
        mutator.execute();
        log.info("Locked:" + id + " with worker:" + workerId);
    }

    private Long smallestToken(List<Long> longs) {
        Collections.sort(longs);
        if(longs.size() >= 2){
            //Check for collisions
            if(longs.get(0) == longs.get(1)){
                return  -1l;
            }
        }
        return longs.size() == 0 ? 0 : longs.get(0);
    }

    private void removeToken(UUID lockId, String requestorId) {
        Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
        mutator.addDeletion(lockId, CF_LOCK_TOKEN, requestorId, SSER);
        mutator.execute();
    }

    private List<String> workersChoosing(UUID lockId) {
        SliceQuery<UUID, String, String> sliceQuery = HFactory.createSliceQuery(keyspace(), UUIDSER, SSER, SSER);
        sliceQuery.setKey(lockId);
        sliceQuery.setColumnFamily(CF_LOCK_CHOOSING);
        sliceQuery.setRange("", "", false, 10);
        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
        ColumnSlice<String, String> slice = result.get();
        List<HColumn<String,String>> columns = slice.getColumns();
        List<String> clients = new ArrayList<String>();
        for (HColumn<String, String> column : columns) {
            clients.add(column.getName());
        }
        return clients;
    }

    private void removeChoosingFlag(UUID lockId, String requestorId) {
        Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
        mutator.addDeletion(lockId, CF_LOCK_CHOOSING, requestorId, SSER);
        mutator.execute();
    }

    private void writeToken(UUID lockId, String requestorId, Long token, int ttl) {
        Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
        HColumn<String, Long> tokenColumn = HFactory.createColumn(requestorId, token, SSER, LSER);
        tokenColumn.setTtl(ttl);
        mutator.addInsertion(lockId, CF_LOCK_TOKEN, tokenColumn);
        mutator.execute();
    }

    private Long nextToken(List<Long> tokens) {
        long nextToken = 1;
        if(tokens.size() != 0){
            Collections.sort(tokens);
            nextToken = tokens.get(tokens.size() - 1) + 1;
        }
        return nextToken;
    }

    private List<Long> requestorTokens(UUID lockId, String workerId) {
        SliceQuery<UUID, String, Long> sliceQuery = HFactory.createSliceQuery(keyspace(), UUIDSER, SSER, LSER);
        sliceQuery.setKey(lockId);
        sliceQuery.setColumnFamily(CF_LOCK_TOKEN);
        sliceQuery.setRange("", "", false, 10);
        QueryResult<ColumnSlice<String, Long>> result = sliceQuery.execute();
        ColumnSlice<String, Long> slice = result.get();
        List<HColumn<String, Long>> columns = slice.getColumns();
        List<Long> tokens = new ArrayList<Long>();
        if(columns.size() > 0){
            for (HColumn<String, Long> column : columns) {
                if(column.getValue() == null){
                    continue;
                }
                tokens.add(column.getValue());
            }
        }
        return tokens;
    }

    private void addChoosingFlag(UUID lockId, String requestorId) {
        Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
        HColumn<String, String> choosingCol = HFactory.createStringColumn(requestorId, "1");
        choosingCol.setTtl(TTL_CHOOSING_SEC);
        mutator.addInsertion(lockId, CF_LOCK_CHOOSING, choosingCol);
        mutator.execute();

    }

    public Set<UUID> findLocksByType(String lockType, String workerId, int minJobs) {
        Set<UUID> locks = new HashSet<UUID>();
        if(emptySlots.containsKey(lockType)){
            return locks;
        }
        SliceQuery<String, UUID, Long> sliceQuery = HFactory.createSliceQuery(keyspace(), SSER, UUIDSER, LSER);
        int attempts = 0;
        List<Integer> slots = new ArrayList<Integer>(SLOTS);
        while (locks.size() < minJobs && slots.size() > 0){
            attempts++;
            Integer slot = slots.remove((int) (Math.random() * (slots.size() - 1)));
            String lockTypeKey = lockTypeKey(lockType, slot);
            if(emptySlots.containsKey(lockTypeKey)){
                continue;
            }
            findLocksInSlot(locks, sliceQuery, lockTypeKey);
            if(locks.size() == 0){
                emptySlots.put(lockTypeKey, System.currentTimeMillis());
            }
        }
        if(locks.size() == 0){
            emptySlots.put(lockType, System.currentTimeMillis());
        }
        return locks;
    }

    private void findLocksInSlot(Set<UUID> locks, SliceQuery<String, UUID, Long> sliceQuery, String lockTypeKey) {
        sliceQuery.setKey(lockTypeKey);
        sliceQuery.setColumnFamily(CF_TYPE_LOCK);
        sliceQuery.setRange(null, null, false, 100);
        QueryResult<ColumnSlice<UUID, Long>> result = sliceQuery.execute();
        ColumnSlice<UUID, Long> columnSlice = result.get();
        List<HColumn<UUID, Long>> columns = columnSlice.getColumns();
        for (HColumn<UUID, Long> column : columns) {
            if(column == null || column.getName() == null){
                continue;
            }
           locks.add(column.getName());
        }
    }

    @Override
    public void complete(UUID lockId, String workerId) {
        if(lockId == null){
            return;
        }
        LockInfo job = getLockInfo(lockId, false);
        if(workerId.equals(job.getLockedBy())){
            Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
            mutator.addDeletion(lockId, CF_LOCK_QUEUE);
            mutator.execute();
            deleteFromProcessor(job);
            log.info("Completed:" + lockId + " with worker:" + workerId);
        }else {
            log.info("Unable to complete:" + lockId + " with worker:" + workerId);
        }
    }

    private void deleteFromProcessor(LockInfo job) {
        Mutator<String> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), SSER);
        mutator.addDeletion(lockTypeKey(job.getLockType(), Integer.parseInt(job.getSlot())), CF_TYPE_LOCK, job.getId(), UUIDSER);
        mutator.execute();
    }

    @Override
    public void release(UUID lockId, String workerId) {
        LockInfo job = getLockInfo(lockId, false);
        if(workerId.equals(job.getLockedBy())){
            Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
            mutator.addDeletion(lockId, CF_LOCK_TOKEN);
            mutator.addDeletion(lockId, CF_LOCK_CHOOSING);
            mutator.execute();
        }
    }

    private LockInfo getLockInfo(UUID lockId, boolean loadProps){
        SliceQuery<UUID, String, String> sliceQuery = HFactory.createSliceQuery(keyspace(), UUIDSER, SSER, SSER);
        sliceQuery.setKey(lockId);
        sliceQuery.setColumnFamily(CF_LOCK_QUEUE);
        sliceQuery.setColumnNames(CF_LOCK_QUEUE, COL_LOCK_TYPE, COL_TYPE_SLOT, COL_LOCKED_BY, COL_CREATED_AT, COL_LOCKED_AT);
        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
        ColumnSlice<String, String> slice = result.get();
        LockInfo job = LockInfo.NULL_LOCK;
        if(slice != null){
            job = new LockInfo(lockId,
                    columnValueOrNull(slice, COL_LOCK_TYPE),
                    columnValueOrNull(slice, COL_TYPE_SLOT),
                    columnValueOrNull(slice, COL_LOCKED_BY),
                    util.fromISOUtcString(columnValueOrNull(slice, COL_CREATED_AT)),
                    util.fromISOUtcString(columnValueOrNull(slice, COL_LOCKED_AT)),
                    loadProps ? lockProperties(lockId) : null);
        }
        return job;
    }
    public LockInfo getLock(UUID jobId) {
        LockInfo job = LockInfo.NULL_LOCK;
        if(jobId == null){
            return job;
        }
        return getLockInfo(jobId, true);
    }

    @Override
    public Set<LockInfo> locksByType(String lockType) {
        Set<UUID> ids = findLocksByType(lockType, null, Integer.MAX_VALUE);
        Set<LockInfo> locks = new HashSet<LockInfo>();
        for (UUID id : ids) {
            locks.add(getLockInfo(id, false));
        }
        return locks;
    }

    private String columnValueOrNull(ColumnSlice<String, String> slice, String columnName) {
        HColumn<String, String> column = slice.getColumnByName(columnName);
        return column != null ? column.getValue() : null;
    }

    @Override
    public void error(UUID lockId, String lockType, String workerId, String message) {
        LockInfo job = getLockInfo(lockId, false);
        if(workerId.equals(job.getLockedBy())){
            Mutator<UUID> mutator = HFactory.createMutator(createKeyspace(getKeyspace(), getClusterHandle()), UUIDSER);
            mutator.addInsertion(lockId, CF_LOCK_ERROR_QUEUE, HFactory.createStringColumn(COL_MESSAGE, message));
            mutator.addDeletion(lockId, CF_LOCK_QUEUE);
            mutator.execute();
        }
    }

}
