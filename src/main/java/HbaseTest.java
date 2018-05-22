import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HbaseTest {
  public static final LinkedBlockingQueue<String> strlist = new LinkedBlockingQueue<String>();
  private static final LinkedBlockingQueue<File> filelist = new LinkedBlockingQueue<File>();
  private static final Log log = LogFactory.getLog(HbaseTest.class);
  public static final ConcurrentSet<Long> responseTime = new ConcurrentSet<>();

  private static String getDateStr() {
    return DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
  }

  public static void main(String[] args) throws Exception {
    String path = null;
    int threads = 0;
    String zookeeper = null;
    String port = null;
    String parent = null;
    String tablename = null;
    String family = null;
    String qualiy = null;
    int batch = 0;

    if (args.length != 9) {
      log.warn("args length is incorrect, please input again !");
      log.info("args 0: file path");
      log.info("args 1: thread numbers");
      log.info("args 2: hbase zookeeper");
      log.info("args 3: hbase port");
      log.info("args 4: hbase parent");
      log.info("args 6: hbase table name");
      log.info("args 7: hbase table family");
      log.info("args 8: hbase table quality");
      log.info("args 9: batch numbers");

      System.exit(0);
      Runtime.getRuntime().gc();
    }

    path = args[0];
    threads = Integer.parseInt(args[1]);
    zookeeper = args[2];
    port = args[3];
    parent = args[4];
    tablename = args[5];
    family = args[6];
    qualiy = args[7];
    batch = Integer.parseInt(args[8]);

    FilesList(path);
    ReadFile();

    List<Thread> threadList = Lists.newArrayList();
    for (int i = 0; i < threads; i++) {
      BatchGet bg = new BatchGet(zookeeper, port, parent, tablename, family, qualiy, batch);
      Thread td = new Thread(bg);
      td.start();
      threadList.add(td);
      log.info("Start thread " + i);
    }

    System.out.println("Test start time: " + getDateStr());
    for (Thread t : threadList) {
      try {
        t.join();
      } catch (Exception e) {
        log.error("Join thread fail", e);
      }
    }

    print("Test end time: " + getDateStr());
    List<Long> timeList = new ArrayList<>(responseTime);
    calcResult(timeList);
    print("10%~90% result:");
    timeList = timeList.subList((int)(timeList.size()*0.1), (int)(timeList.size()*0.9));
    calcResult(timeList);
  }

  private static void print(String str) {
    System.out.println(str);
  }

  private static void calcResult(List<Long> timeList) {
    long total = 0;
    int size = timeList.size();
    for (long time : timeList) {
      total += time;
    }
    print("Average Delay: " + (total / size) + " ms  Size: " + size);
    print(String.format("Percentile 0.50, 0.75, 0.90, 0.95, 0.99: %d %d %d %d %d",
        timeList.get((int)(size*0.5)), timeList.get((int)(size*0.75)),
        timeList.get((int)(size*0.90)),timeList.get((int)(size*0.95)),timeList.get((int)(size*0.99))));
  }

  private static void FilesList(String dir) {
    if (null != dir && dir.trim().length() > 0) {
      File file = new File(dir.trim());
      if (file.isDirectory()) {
        File[] listFile = file.listFiles();
        if (null != listFile) {
          for (File f : listFile) {
            if (f.isFile()) {
              filelist.add(f);
            }
          }
        }
      } else {
        filelist.add(file);
      }
    }
  }

  private static void ReadFile() {
    while (true) {
      File file = null;
      try {
        file = filelist.poll(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (null == file) {
        break;
      }

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
          strlist.add(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    log.info("File lines: " +  strlist.size());
  }

}