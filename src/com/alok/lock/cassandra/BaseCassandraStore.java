package com.alok.lock.cassandra;

import com.alok.lock.util.Util;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.UUID;

import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;

public class BaseCassandraStore {

    protected static final Logger log = LogManager.getLogger(BaseCassandraStore.class);

    private final String host;
    private final int port;
    private final String clusterName;
    private final String keyspace;
    protected static final Serializer<String> SSER = StringSerializer.get();
    protected static final Serializer<byte[]> BASR = BytesArraySerializer.get();
    protected static final Serializer<UUID> UUIDSER = UUIDSerializer.get();
    protected static final Serializer<Long> LSER = LongSerializer.get();
    private final CassandraHostConfigurator cfg;
    protected final Util util;

    private int maxConnections = 500;

    public BaseCassandraStore(Properties props) {
        this(props.getProperty("cassandra.hosts"),
                Integer.parseInt(props.getProperty("cassandra.port")),
                props.getProperty("cassandra.cluster"),
                props.getProperty("cassandra.keyspace"),
                Boolean.parseBoolean(props.getProperty("recordStats")));
        if(props.containsKey("cassandra.maxConnections")){
            this.maxConnections = Integer.parseInt(props.getProperty("cassandra.maxConnections"));
        }
     }

    public BaseCassandraStore(String host, int port, String clusterName, String keyspace, boolean recordStats) {
        this.host = host;
        this.port = port;
        this.clusterName = clusterName;
        this.keyspace = keyspace;
        this.cfg = new CassandraHostConfigurator(host);
        this.cfg.setPort(port);
        this.util = new Util();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private String getCluster() {
        return "Test Cluster";
    }

    protected Cluster getClusterHandle() {
        CassandraHostConfigurator configurator = new CassandraHostConfigurator();
        configurator.setHosts(getHost());
        configurator.setPort(getPort());
        configurator.setMaxActive(getMaxConnections());
        return HFactory.getOrCreateCluster(getCluster(), getHost() + ":" + getPort());
    }

    protected long lValue(ColumnSlice<String, byte[]> row, String colName) {
        byte[] bytes = bValue(row, colName);
        return bytes == null ? 0l: LSER.fromBytes(bytes);
    }

    protected String sValue(ColumnSlice<String, byte[]> row, String colName) {
        byte[] bytes = bValue(row, colName);
        return bytes == null ? "": new String(bytes);
    }

    protected String sValueOrEmpty(ColumnSlice row, String colName) {
        Object rawValue = null;
        String value = "";
        if(row.getColumnByName(colName) != null){
            rawValue = row.getColumnByName(colName).getValue();
            if(rawValue instanceof byte[]){
                value = new String((byte[]) rawValue);
            }else {
                value = String.valueOf(rawValue);
            }
        }
        return value;
    }

    protected byte[] bValue(ColumnSlice<String, byte[]> row, String colName) {
        if(row.getColumnByName(colName) == null){
            System.out.println("Col missing:" + colName);
            return null;
        }
        return row.getColumnByName(colName).getValue();
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getKeyspace() {
        return keyspace;
    }

    protected Keyspace keyspace() {
        return createKeyspace(getKeyspace(), getClusterHandle(), new QuorumAllConsistencyLevelPolicy());
    }

    public int getMaxConnections() {
        return maxConnections;
    }
}
