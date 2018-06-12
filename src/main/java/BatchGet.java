import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchGet implements Runnable {
  private static final Log log = LogFactory.getLog(BatchGet.class);

  Connection connection;
  String _tablename = null;
  String _family = null;
  String _quality = null;
  byte[] _FAMILY = null;
  byte[] _QUALITY = null;
  int _batch = 0;

  public BatchGet(Connection connection, String _tablename, String _family, String _quality, int _batch) {
    this.connection = connection;
    this._tablename = _tablename;
    this._family = _family;
    this._quality = _quality;

    if (null != _family) {
      _FAMILY = Bytes.toBytes(_family);
    } else {
      log.error("family is null !");
    }
    if (null != _quality) {
      _QUALITY = Bytes.toBytes(_quality);
    } else {
      log.error("quality is null !");
    }

    this._batch = _batch;
  }

  @Override
  public void run() {
    batch2Hbase();
  }

  private void batch2Hbase() {
    List<Row> batch = new ArrayList<Row>();

    String str = null;
    int count = 0;
    while (true) {
      try {
        str = HbaseTest.strlist.poll(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (null == str) {
        log.warn("get null line data");
        break;
      }

      byte[] rowkey = Bytes.toBytes(str);
      Get get = new Get(rowkey);
      get.addColumn(_FAMILY, _QUALITY);
      batch.add(get);

      int size = batch.size();
      if (size > 0 && (size % _batch == 0)) {
        Table ht = null;
        long begin = System.currentTimeMillis();
        long dataSize = 0;
        try {
          ht = connection.getTable(TableName.valueOf(_tablename));
          Object[] results = ht.batch(batch);
          for (Object result : results) {
            dataSize += getRowSize((Result)result);
          }
          count++;
        } catch (Throwable t) {
          log.error("batchGetFail", t);
        } finally {
          long end = System.currentTimeMillis();
          long cust = end - begin;
          if (ht != null) {
            try {
              ht.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          batch.clear();
          log.info("batch: " + size + ", time: " + cust);
          if (count == 1)
            continue;
          HbaseTest.addTime(cust);
          HbaseTest.addDataSize(dataSize);
        }
      }
    }
    log.info("ThreadRunLoop: " + count);

  }

  private long getRowSize(Result result) {
    long size = 0;
    if (result != null) {
      byte[] value = result.getValue(_FAMILY, _QUALITY);
      if (value != null) {
        size += value.length;
      }
      System.out.println(String.valueOf(value));
    }

    return size;
  }
}