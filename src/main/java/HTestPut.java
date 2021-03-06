import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class HTestPut {

    public static String format = "yyyy-MM-dd HH:mm:ss";
    public static final LinkedBlockingQueue<String> keylist = new LinkedBlockingQueue<String>();
    public static MetricRegistry registry = new MetricRegistry();
    public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    public static Timer t = registry.timer("test");

    public static String timeStamp2Date(long seconds) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(seconds));
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
        options.addOption("q", "qualify", true, "File to save program output to");
        options.addOption("F", "file", true, "File to save program output to");
        options.addOption("b", "batchSize", true, "File to save program output to");
        // Parse the program arguments
        CommandLine commandLine = parser.parse(options, args);

        String zookeeper = commandLine.getOptionValue("z", "zk-1.static.bjs-datalake.p1staff.com");
        String port = commandLine.getOptionValue("P", "2181");
        String parent = commandLine.getOptionValue("p", "/hbase-antispam");
        String tablename = commandLine.getOptionValue("t", "device_info");
        String family = commandLine.getOptionValue("f", "cf");
        String qualify = commandLine.getOptionValue("q", "data");
        String file = commandLine.getOptionValue("F", "features");
        int batchSize = Integer.valueOf(commandLine.getOptionValue("b", "250"));

        reporter.start(10, TimeUnit.SECONDS);

        System.out.println("+++++++++++++++++++++++++++++++++");
        System.out.println(zookeeper);
        System.out.println(port);
        System.out.println(parent);
        System.out.println(tablename);
        System.out.println(family);
        System.out.println(qualify);
        System.out.println(file);
        System.out.println(batchSize);
        System.out.println("+++++++++++++++++++++++++++++++++");
        Connection connection = ConnectionFactory.createConnection(HbaseConnect.connection(zookeeper, parent, port));
//        testBatchGet(family, qualify, tablename, connection);
//        testGet(family, qualify, tablename, connection);

        Table table = connection.getTable(TableName.valueOf("device_info"));
        Put put = new Put(Bytes.toBytes(1000));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualify), Bytes.toBytes("hbase test put"));
        table.put(put);
    }

}
