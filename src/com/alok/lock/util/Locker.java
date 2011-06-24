package com.alok.lock.util;

import com.alok.lock.cassandra.CassandraLockStore;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Locker implements Callable<String> {
    private static final Logger log = LogManager.getLogger(Locker.class);

    public static final int SECOND = 1000;
    private int runTime;
    private int locksPerSecond;
    private CassandraLockStore store;
    private String lockType;
    private String workerId;
    private StringBuilder data;
    private AtomicLong counter;

    public Locker(Properties props, AtomicLong counter) {
        this.counter = counter;
        this.runTime = Integer.parseInt(props.getProperty("run.time", "60"));
        this.locksPerSecond = Integer.parseInt(props.getProperty("lock.maxCreatePerSecond", "1000"));
        this.lockType = props.getProperty("lock.type", "FOO");
        this.store = new CassandraLockStore(props);
        this.workerId = UUID.randomUUID().toString();
        this.data = new StringBuilder();

    }

    public String call(){
        long start = System.currentTimeMillis();
        long loopStart = System.currentTimeMillis();
        while((System.currentTimeMillis() - start) <= (runTime*SECOND)){
            long startTime = System.currentTimeMillis();
            UUID lock = store.lock(lockType, workerId, 60);
            int lockAttempts = 0;
            while(lock == null && lockAttempts < 100){
                lock = store.lock(lockType, workerId, 60);
                lockAttempts++;
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(lock == null){
                continue;
            }
            counter.addAndGet(1);
            long now = System.currentTimeMillis();
            log("LockTime:[" + (now - startTime) + "]");
            try{
                store.complete(lock, workerId);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return data.toString();
    }

    private void log(String msg) {
        data.append(msg).append("\n\r");
        System.out.println(msg);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));
        int lockerThreadCount = Integer.parseInt(props.getProperty("locker.threads", "2"));
        ExecutorService executorService = Executors.newFixedThreadPool(lockerThreadCount);
        AtomicLong counter = new AtomicLong(0);
        List<Callable<String>> callables = new ArrayList<Callable<String>>();
        for(int i = 0; i < lockerThreadCount; i++){
            callables.add(new Locker(props, counter));
        }
        long start = System.currentTimeMillis();
        List<Future<String>> futures = executorService.invokeAll(callables);
        for (Future<String> future : futures) {
            try {
                String data = future.get();
                System.out.println("Locks:[" + counter.get() + "], time ms:[" + (System.currentTimeMillis() -start) + "]");
            } catch (ExecutionException e) {
            }

        }
        System.exit(0);
    }
}
