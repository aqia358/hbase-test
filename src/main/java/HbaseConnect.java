import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConnect {
  public static Configuration connection(String zookeeper, String parent, String port) {
    Configuration HBASE_CONFIG = new Configuration();
    HBASE_CONFIG.set("hbase.zookeeper.quorum", zookeeper);
    HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", port);
    HBASE_CONFIG.set("zookeeper.znode.parent", parent);
    HBASE_CONFIG.setLong("zookeeper.session.timeout", 900000);
    HBASE_CONFIG.setInt("hbase.client.retries.number", 5);
    HBASE_CONFIG.setInt("hbase.meta.scanner.caching", 5000);
    HBASE_CONFIG.setInt("hbase.client.prefetch.limit", 100);
    HBASE_CONFIG.setInt("hbase.rpc.timeout", 600000);

    HBASE_CONFIG.setBoolean("hbase.ipc.client.allowsInterrupt", true);
    HBASE_CONFIG.setInt("hbase.client.primaryCallTimeout.get", 10000);
    HBASE_CONFIG.setInt("hbase.client.primaryCallTimeout.multiget", 10000);
    HBASE_CONFIG.setInt("hbase.client.replicaCallTimeout.scan", 100000);

    Configuration configuration = HBaseConfiguration.create(HBASE_CONFIG);
    return configuration;
  }
}

