import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class HTest {

    public static final LinkedBlockingQueue<String> keylist = new LinkedBlockingQueue<String>();
    public static MetricRegistry registry = new MetricRegistry();
    public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    public static Timer t = registry.timer("test");

    private static void readFile(String path) {
        File file = new File(path.trim());
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
        BufferedReader reader = new BufferedReader(isr);

        String line = null;
        try {
            while (null != (line = reader.readLine())) {
                keylist.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        String keys = "";
//        String[] a = keys.split(",");
//        for (String tmp: Arrays.asList(keys.split(",")))
//            keylist.add(tmp);
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("z", "zookeeper", false, "Print out VERBOSE information");
        options.addOption("p", "parent", true, "File to save program output to");
        options.addOption("P", "port", true, "File to save program output to");
        options.addOption("t", "tablename", true, "File to save program output to");
        options.addOption("f", "family", true, "File to save program output to");
        options.addOption("F", "file", true, "File to save program output to");
        options.addOption("b", "batchSize", true, "File to save program output to");
        // Parse the program arguments
        CommandLine commandLine = parser.parse(options, args);

        String zookeeper = commandLine.getOptionValue("z", "10.189.200.45");
        String port = commandLine.getOptionValue("P", "2181");
        String parent = commandLine.getOptionValue("p", "/hbase");
        String tablename = commandLine.getOptionValue("t", "fg_user_features_hbase");
        String family = commandLine.getOptionValue("q", "f");
        String qualiy = commandLine.getOptionValue("q", "features");
        String file = commandLine.getOptionValue("F", "features");
        int batchSize = Integer.valueOf(commandLine.getOptionValue("b", "250"));

        reporter.start(10, TimeUnit.SECONDS);

        readFile(file);

        Connection connection = ConnectionFactory.createConnection(HbaseConnect.connection(zookeeper, parent, port));
//        testBatchGet(family, qualiy, tablename, connection);
//        testGet(family, qualiy, tablename, connection);
        testBatch(family, qualiy, tablename, connection, batchSize);

    }

    public static void testBatchGet(String family, String qualiy, String tablename, Connection connection) throws Exception {
        byte[] hFamily = Bytes.toBytes(family);
        byte[] hQualiy = Bytes.toBytes(qualiy);
        Table ht = connection.getTable(TableName.valueOf(tablename));
        for (int i = 0; i < 100; i++) {
            for (String keys : keylist) {
                List<Row> batch = new ArrayList<Row>();
                for (String key : keys.split(",")) {
                    byte[] rowkey = Bytes.toBytes(key);
                    Get get = new Get(rowkey);
                    get.addColumn(hFamily, hQualiy);
                    batch.add(get);
                }
                t.time((Callable<Void>) () -> {
                    long s = System.currentTimeMillis();
                    Object[] results = new Object[batch.size()];
                    ht.batch(batch, results);
                    long e = System.currentTimeMillis();
                    System.out.println("result:" + results.length + ", time:" + (e - s));
                    return null;
                });
            }
        }

    }

    public static void testBatch(String family, String qualiy, String tablename, Connection connection, int batchSize) throws Exception {
        byte[] hFamily = Bytes.toBytes(family);
        byte[] hQualiy = Bytes.toBytes(qualiy);
        Table ht = connection.getTable(TableName.valueOf(tablename));
        for (int i = 0; i < 10000; i++) {
            System.out.println("------------------NO " + i + " round--------------------");
            List<Row> batch = new ArrayList<Row>();
            for (String key : keylist) {
                if (batch.size() > batchSize) {
                    t.time((Callable<Void>) () -> {
                        long s = System.currentTimeMillis();
                        Object[] results = new Object[batch.size()];
                        ht.batch(batch, results);
                        long e = System.currentTimeMillis();
                        System.out.println("result:" + results.length + ", time:" + (e - s));
                        return null;
                    });
                    batch.clear();
                } else {
                    byte[] rowkey = Bytes.toBytes(key);
                    Get get = new Get(rowkey);
                    get.addColumn(hFamily, hQualiy);
                    batch.add(get);
                }
            }
        }

    }

    public static void testGet(String family, String qualiy, String tablename, Connection connection) throws Exception {
        byte[] hFamily = Bytes.toBytes(family);
        byte[] hQualiy = Bytes.toBytes(qualiy);
        Table ht = connection.getTable(TableName.valueOf(tablename));
        for (int i = 0; i < 100; i++) {
            for (String keys : keylist) {
                List<Row> batch = new ArrayList<Row>();
                for (String key : keys.split(",")) {
                    byte[] rowkey = Bytes.toBytes(key);
                    Get get = new Get(rowkey);
                    get.addColumn(hFamily, hQualiy);
                    batch.add(get);
                    t.time((Callable<Void>) () -> {
                        long s = System.currentTimeMillis();
                        Result result = ht.get(get);
                        long e = System.currentTimeMillis();
                        System.out.println("result:" + 1 + ", time:" + (e - s));
                        return null;
                    });
                }

            }
        }

    }

//    Result result = table.get(get);

}
