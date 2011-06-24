package com.alok.lock.util;

import com.alok.lock.cassandra.CassandraLockStore;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LockCreator extends Thread {

    private static final Logger log = LogManager.getLogger(LockCreator.class);

    public static final int SECOND = 1000;
    private int runTime;
    private int locksPerSecond;
    private CassandraLockStore store;
    private String lockType;



    public LockCreator(Properties props){
        this.runTime = Integer.parseInt(props.getProperty("run.time", "60"));
        this.locksPerSecond = Integer.parseInt(props.getProperty("lock.maxCreatePerSecond", "1000"));
        this.lockType = props.getProperty("lock.type", "FOO");
        this.store = new CassandraLockStore(props);
    }

    public void run(){
        long start = System.currentTimeMillis();
        long count  =0;
        long loopStart = System.currentTimeMillis();
        long total = 0;
        while((System.currentTimeMillis() - start) <= (runTime*SECOND)){
            store.add(lockType, randomProperties());
            count++;
            total++;
            if(count >= locksPerSecond){
                long now = System.currentTimeMillis();
                if((now - loopStart) < SECOND){
                    try {
                        sleep(1000-(now - loopStart));
                    } catch (InterruptedException e) {
                    }
                }
                System.out.println("Jobs:[" + count + "], Time (ms) :[" + (now - loopStart) + "], Total:[" + total + "]");
                count = 0;
                loopStart = System.currentTimeMillis();
            }
        }
    }

    private Map<String, String> randomProperties() {
        Map<String, String> props = new HashMap<String, String>();
        props.put("name1", "val1");
        props.put("name2", "val1");
        props.put("name3", "val1");
        props.put("name4", "val1");
        props.put("name5", "val1");
        props.put("name6", "val1");
        props.put("name7", "val1");
        props.put("name8", "val1");
        return props;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));
        LockCreator creator = new LockCreator(props);
        creator.start();
        creator.join();
        System.exit(0);
    }
}
