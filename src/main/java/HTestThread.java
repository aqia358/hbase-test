import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.*;
import org.apache.htrace.impl.HBaseSpanReceiver;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class HTestThread {

    static {
        PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf" + File.separator + "log4j.properties");
    }


    public static String format = "yyyy-MM-dd HH:mm:ss";
    public static final LinkedBlockingQueue<String> keylist = new LinkedBlockingQueue<String>();

    private static final Log log = LogFactory.getLog(HTestThread.class);

    public static String timeStamp2Date(long seconds) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(seconds));
    }

    private static LinkedBlockingQueue<String> readFile(String path) {
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
        return keylist;
    }

    public static void main(String[] args) throws Exception {
        Logger.getRootLogger().setLevel(Level.DEBUG);
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
        options.addOption("T", "thread", true, "File to save program output to");
        options.addOption("n", "nThread", true, "File to save program output to");
        // Parse the program arguments
        CommandLine commandLine = parser.parse(options, args);

        String zookeeper = commandLine.getOptionValue("z", "10.189.200.45");
        String port = commandLine.getOptionValue("P", "2181");
        String parent = commandLine.getOptionValue("p", "/hbase");
        String tablename = commandLine.getOptionValue("t", "fg_user_features_hbase");
        String family = commandLine.getOptionValue("q", "f");
        String qualiy = commandLine.getOptionValue("f", "features");
        String file = commandLine.getOptionValue("F", "features");
        int batchSize = Integer.valueOf(commandLine.getOptionValue("b", "250"));
        int threads = Integer.valueOf(commandLine.getOptionValue("T", "10"));
        int nThreads = Integer.valueOf(commandLine.getOptionValue("n", "10"));


        nThreads = Runtime.getRuntime().availableProcessors() * 4;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true).setNameFormat("liuhl-user-%d").build();
        ExecutorService service = new ForkJoinPool(nThreads * 2);
        Configuration conf = HbaseConnect.connection(zookeeper, parent, port);
        Connection connection = ConnectionFactory.createConnection(conf, service);
        TableName tableName = TableName.valueOf(tablename);
        warmUpConnectionCache(connection, tableName);

        ExecutorService executor = Executors.newFixedThreadPool(nThreads, threadFactory);
        for (int i = 0; i < threads; i++) {
            HbaseJob worker = new HbaseJob(connection, family, qualiy, tablename, batchSize, file, "liuhl_hbase_thread_" + i, conf);
            executor.execute(worker);
        }
        while (!executor.isTerminated()) {
        }
        executor.shutdown();
        service.shutdownNow();
        System.out.println("Finished all threads");

    }

    public static void warmUpConnectionCache(Connection connection, TableName tn) throws IOException {
        try (RegionLocator locator = connection.getRegionLocator(tn)) {
            log.info(
                    "Warmed up region location cache for " + tn
                            + " got " + locator.getAllRegionLocations().size());
        }
    }

    static class HbaseJob implements Runnable {
        private Connection connection;
        private String family;
        private String qualiy;
        private String tablename;
        private int batchSize;
        private String name;
        private Configuration conf;
        public LinkedBlockingQueue<String> keys;
        public static MetricRegistry registry = new MetricRegistry();
        public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        public static Timer t;

        public HbaseJob(Connection connection, String family, String qualiy, String tablename, int batchSize, String path, String name, Configuration conf) {
            this.connection = connection;
            this.family = family;
            this.qualiy = qualiy;
            this.tablename = tablename;
            this.batchSize = batchSize;
            this.name = name;
            keys = readFile(path);
            reporter.start(10000, TimeUnit.SECONDS);
            t = registry.timer(name);
            this.conf = conf;
        }

        @Override
        public void run() {
            try {
                System.out.println(Thread.currentThread().getName() + " End.");
                initHtrace();
                testBatch(family, qualiy, tablename, connection, batchSize);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void initHtrace() {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "2.hbase.master.bjs-datalake.p1staff.com,3.hbase.master.bjs-datalake.p1staff.com,1.hbase.master.bjs-datalake.p1staff.com");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.znode.parent", "/hbase-standby");
            conf.set("hbase.htrace.hbase.collector-quorum", "2.hbase.master.bjs-datalake.p1staff.com,3.hbase.master.bjs-datalake.p1staff.com,1.hbase.master.bjs-datalake.p1staff.com");
            conf.set("hbase.htrace.hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.htrace.hbase.zookeeper.znode.parent", "/hbase-standby");
            SpanReceiverBuilder builder = new SpanReceiverBuilder(new HBaseHTraceConfiguration(conf));
            SpanReceiver receiver = builder.spanReceiverClass(HBaseSpanReceiver.class.getName()).build();
            Trace.addReceiver(receiver);

        }

        public void testBatch(String family, String qualiy, String tablename, Connection connection, int batchSize) throws Exception {
            byte[] hFamily = Bytes.toBytes(family);
            byte[] hQualiy = Bytes.toBytes(qualiy);
            for (int i = 0; i < 10000; i++) {
                System.out.println(Thread.currentThread().getName() + " End.");
                System.out.println("------------------NO " + i + " round--------------------");
                List<Row> batch = new ArrayList<Row>();
                for (String key : keys) {
                    if (batch.size() > batchSize) {
                        Table ht = connection.getTable(TableName.valueOf(tablename));
                        Object[] results = new Object[batch.size()];
                        long s = System.currentTimeMillis();
                        t.time((Callable<Void>) () -> {
                            SpanReceiverHost.getInstance(conf);
                            TraceScope ts = Trace.startSpan("Gets", Sampler.ALWAYS);
                            try {
                                ht.partialBatch(batch, results, 100000);
                            } finally {
                                ts.close();
                            }
//                            try {
//                                log.info(results);
//                                ht.batch(batch, results);
//                            } catch (IOException e) {
//                                log.error("liuhl hbase batch IOException");
//                            } catch (InterruptedException e) {
//                                log.error("liuhl hbase batch InterruptedException");
//                            }
                            return null;
                        });
                        int nullCount = 0;
                        int staleCount = 0;
                        int unStaleCount = 0;
                        int unknown = 0;
                        for (Object r : results) {
                            if (!(r instanceof Result)) {
                                unknown += 1;
                            }
                            if (r == null) {
                                nullCount += 1;
                                continue;
                            }
                            if (r instanceof Result && ((Result) r).isStale()) {
                                staleCount += 1;
                            } else {
                                unStaleCount += 1;
                            }
                        }
                        long e = System.currentTimeMillis();
                        log.info("liuhl stale_count:" + staleCount + ", unstale_count:" + unStaleCount + ", null_count:" + nullCount + ", unkonw:" + unknown + ", time:" + (e - s));
//                            System.out.println(name + " start time:" + timeStamp2Date(s) + ", result:" + results.length + ", time:" + (e - s));
                        log.info(name + " start time:" + timeStamp2Date(s) + ", result:" + results.length + ", time:" + (e - s));
                        batch.clear();
                    } else {
                        byte[] rowkey = Bytes.toBytes(key);
                        Get get = new Get(rowkey);
                        get.setConsistency(Consistency.TIMELINE);
                        get.addColumn(hFamily, hQualiy);
                        batch.add(get);
                    }
                }
            }

        }


    }

}
