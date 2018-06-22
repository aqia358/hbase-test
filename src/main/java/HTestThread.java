import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import sun.jvm.hotspot.utilities.WorkerThread;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class HTestThread {

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




        Connection connection = ConnectionFactory.createConnection(HbaseConnect.connection(zookeeper, parent, port));

        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            HbaseJob worker = new HbaseJob(connection, family, qualiy, tablename, batchSize, file, "thread_" + i);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
//
//        List<Thread> threadList = Lists.newArrayList();
//        for (int i = 0; i < threads; i++) {
//            HbaseJob bg = new HbaseJob(connection, family, qualiy, tablename, batchSize, file, "thread_" + i);
//            Thread td = new Thread(bg);
//            td.start();
//            threadList.add(td);
//            log.info("Start thread " + i);
//        }
//
//        long startTime = System.currentTimeMillis();
//        System.out.println("Test start time: " + timeStamp2Date(startTime));
//        for (Thread t : threadList) {
//            try {
//                t.join();
//            } catch (Exception e) {
//                log.error("Join thread fail", e);
//            }
//        }

    }

    static class HbaseJob implements Runnable {
        private Connection connection;
        private String family;
        private String qualiy;
        private String tablename;
        private int batchSize;
        public LinkedBlockingQueue<String> keys;
        public static MetricRegistry registry = new MetricRegistry();
        public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        public static Timer t ;

        public HbaseJob(Connection connection, String family, String qualiy, String tablename, int batchSize, String path, String name) {
            this.connection = connection;
            this.family = family;
            this.qualiy = qualiy;
            this.tablename = tablename;
            this.batchSize = batchSize;
            keys = readFile(path);
            reporter.start(10, TimeUnit.SECONDS);
            t = registry.timer(name);
        }

        @Override
        public void run() {
            try {
                testBatch(family, qualiy, tablename, connection, batchSize);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void testBatch(String family, String qualiy, String tablename, Connection connection, int batchSize) throws Exception {
            byte[] hFamily = Bytes.toBytes(family);
            byte[] hQualiy = Bytes.toBytes(qualiy);
            Table ht = connection.getTable(TableName.valueOf(tablename));
            for (int i = 0; i < 10000; i++) {
                System.out.println("------------------NO " + i + " round--------------------");
                List<Row> batch = new ArrayList<Row>();
                for (String key : keys) {
                    if (batch.size() > batchSize) {
                        t.time((Callable<Void>) () -> {
                            long s = System.currentTimeMillis();
                            Object[] results = new Object[batch.size()];
                            ht.batch(batch, results);
                            long e = System.currentTimeMillis();
                            System.out.println("start time:" + timeStamp2Date(s) + "end time:" + timeStamp2Date(e) + ", result:" + results.length + ", time:" + (e - s));
                            log.info("start time:" + timeStamp2Date(s) + "end time:" + timeStamp2Date(e) + ", result:" + results.length + ", time:" + (e - s));
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


    }

}
