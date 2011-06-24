package com.alok.lock;

import com.alok.lock.cassandra.CassandraLockStore;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class LockStoreTest {
    private LockStore store;


    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.load(getClass().getResourceAsStream("test.properties"));
        store = new CassandraLockStore(props);
    }

    @Test
    public void testCreateLock() throws Exception {
        String lockType = UUID.randomUUID().toString();
        Map<String, String> properties = randomMap();
        UUID lockId = store.add(lockType, properties);
        LockStore.LockInfo lock = store.getLock(lockId);
        Map<String, String> storedProps = lock.getProperties();
        for (String key : properties.keySet()) {
            Assert.assertEquals(properties.get(key), storedProps.get(key));
        }
    }

    @Test
    public void testCreateAndAcquireLock() throws Exception {
        String lockType = UUID.randomUUID().toString();
        store.add(lockType, randomMap());
        String workerId = UUID.randomUUID().toString();
        UUID lock = null;
        while (lock == null){
            lock = store.lock(lockType, workerId, 5);
            Thread.sleep(250);
        }
        Assert.assertNotNull(lock);
        Assert.assertNull(store.lock(lockType, workerId, 5));
        lock = null;
        long time = System.currentTimeMillis();
        while ((lock == null) && (System.currentTimeMillis() - time) < 60000){
            lock = store.lock(lockType, workerId, 5);
            Thread.sleep(10000);
        }
        Assert.assertNotNull(lock);
    }

    @Test
    public void testConcurrentAcquireLocks() throws Exception {
        final Map<String, List<UUID>> lockedLocks = new HashMap<String, List<UUID>>();
        final String lockTypePrefix = "lockType1-" + System.currentTimeMillis() + "-";
        final AtomicLong lockCount = new AtomicLong(0);
//        final String lockTypePrefix = "lockType1-"+ UUID.randomUUID().toString() + "-";
        addLocks(lockTypePrefix, 2000);
        final long time = System.currentTimeMillis();
        List<Callable<Boolean>> workers = new ArrayList<Callable<Boolean>>();
        for(int i = 0; i < 100; i++){
            final ArrayList<UUID> lockList = new ArrayList<UUID>();
            final String workerId = "" + i;
            final int slot = i;
            lockedLocks.put(workerId, lockList);
            workers.add(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Thread.sleep((long) (Math.random() * 1000));
                    String proccesor = lockTypePrefix + (slot%10);
                    int miss = 0;
                    for (int i = 0; i < 250; i++) {
                        try{
//                        String proccesor = lockTypePrefix + ((int)(Math.random() * 9.9));
                        UUID lock = store.lock(proccesor, workerId, 600);
                            if(lock == null){
                                miss++;
//                                addLocks(lockTypePrefix, 500);
                                System.out.println("Processor has no locks:" + proccesor);
                                Thread.sleep((long) (1000*miss));
                                if(miss > 3){
                                    return Boolean.TRUE;
                                }
                            }

                        if (lock != null) {
                            miss = 0;
//                            System.out.println("Time:" + System.currentTimeMillis() + ", Locked:" + lock.toString() + ", lockType:" + proccesor);
                            lockList.add(lock);
                            store.complete(lock, workerId);
                            lockCount.incrementAndGet();
                        }
                        System.out.println("[[[Locks completed:" + lockCount.get() + " in " + (System.currentTimeMillis() - time) + "ms]]");

                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    System.out.println("Worker done: " + workerId);
                    return Boolean.TRUE;
                }
            });
        }
        ExecutorService executorService = Executors.newFixedThreadPool(75);
        executorService.invokeAll(workers);
        int count = 0;
        Set<UUID> lockIds = new HashSet<UUID>();
        for (String workerId : lockedLocks.keySet()) {
            count += lockedLocks.get(workerId).size();
            lockIds.addAll(lockedLocks.get(workerId));
        }
        Map<String, Collection<UUID>> missingLocks = new HashMap<String, Collection<UUID>>();
        for(int i = 0; i < 10; i++){
            missingLocks.put(lockTypePrefix + i, store.findLocksByType(lockTypePrefix + i, "foo", 1));
        }
        System.out.println("Completed Locks: " + lockIds.size());
        for (String proc : missingLocks.keySet()) {
            System.out.println(proc + " Missing Locks: " + missingLocks.get(proc).size());
            for (UUID uuid : missingLocks.get(proc)) {
                System.out.println(store.getLock(uuid));
            }
        }
        System.out.println("Time Taken: " + (System.currentTimeMillis() - time));
        Assert.assertEquals(20000, count);
        Assert.assertEquals(lockIds.size(), count);
    }

    private void addLocks(String lockTypePrefix, int count) {
        for(int i = 0; i < count; i++){
            store.add(lockTypePrefix + 0, randomMap());
            store.add(lockTypePrefix + 1, randomMap());
            store.add(lockTypePrefix + 2, randomMap());
            store.add(lockTypePrefix + 3, randomMap());
            store.add(lockTypePrefix + 4, randomMap());
            store.add(lockTypePrefix + 5, randomMap());
            store.add(lockTypePrefix + 6, randomMap());
            store.add(lockTypePrefix + 7, randomMap());
            store.add(lockTypePrefix + 8, randomMap());
            store.add(lockTypePrefix + 9, randomMap());
            if(i%100 == 0){
                System.out.println("Added Locks:" + (i*10));
            }
        }
    }

    private Map<String, String> randomMap() {
        Map<String, String> props = new HashMap<String, String>();
        props.put("name1", "val1");
        return props;
    }

    public void testData() throws IOException {
         BufferedReader reader = new BufferedReader(new FileReader("/home/alok/work/data.txt"));
         String line = reader.readLine();
         Map<String, String> map = new HashMap<String, String>();
         while(line != null){
             String[] kv = line.split(",");
             if(map.containsKey(kv[0])){
                 System.out.println("Dup:" + kv[0] + "orig:" + map.get(kv[0]) + ", new:" + kv[1]);
             }
             map.put(kv[0], kv[1]);
             line = reader.readLine();
         }

     }
}
